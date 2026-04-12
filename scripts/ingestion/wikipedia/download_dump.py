# scripts/ingestion/download_wiki_dump.py
"""
Production-grade Nepali Wikipedia dump downloader.

Designed for Airflow execution with:
- Network robustness (retries, timeouts, resume support)
- Atomic writes (temp files, safe overwrites)
- Clear idempotency semantics
- Early config validation
- Operator-friendly logging
"""

import hashlib
import shutil
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scripts.utils.paths import RAW_DIR
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# ============================================================================
# LOGGER
# ============================================================================
LOGGER = get_logger("download_wiki", log_type="wikipedia")

# ============================================================================
# CONSTANTS
# ============================================================================
CHUNK_SIZE = 8192 * 8  # 64KB chunks
DOWNLOAD_TIMEOUT = (30, 300)  # (connect, read) timeout in seconds
MAX_RETRIES = 5
BACKOFF_FACTOR = 2  # Exponential backoff


# ============================================================================
# VALIDATION
# ============================================================================
def validate_config(config: Dict[str, Any]):
    """
    Validate required configuration fields.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        KeyError: If required field missing
        ValueError: If field value invalid
    """
    required_fields = [
        ("download", "base_url"),
        ("download", "dump_file"),
        ("paths", "raw_dump_dir"),
    ]
    
    for *path, field in required_fields:
        obj = config
        for key in path:
            if key not in obj:
                raise KeyError(f"Missing required config: {'.'.join(path)}.{field}")
            obj = obj[key]
        
        if field not in obj:
            raise KeyError(f"Missing required config: {'.'.join(path)}.{field}")
        
        if not obj[field]:
            raise ValueError(f"Empty config value: {'.'.join(path)}.{field}")
    
    # Validate URL format
    base_url = config["download"]["base_url"]
    if not base_url.startswith(("http://", "https://")):
        raise ValueError(f"Invalid base_url (must start with http:// or https://): {base_url}")


# ============================================================================
# NETWORK SESSION
# ============================================================================
def create_robust_session() -> requests.Session:
    """
    Create requests session with retry logic and timeouts.
    
    Returns:
        Configured session
    """
    session = requests.Session()
    
    # Retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


# ============================================================================
# ATOMIC DOWNLOAD
# ============================================================================
def download_with_resume(
    url: str,
    output_path: Path,
    session: requests.Session
) -> Dict[str, Any]:
    """
    Download file with resume support and atomic write.
    
    Args:
        url: URL to download
        output_path: Final output path
        session: Requests session
        
    Returns:
        Download metadata (size, duration, resumed)
        
    Raises:
        requests.RequestException: On download failure
    """
    temp_path = output_path.with_suffix(output_path.suffix + ".partial")
    
    # Check if partial download exists
    resume_byte_pos = 0
    if temp_path.exists():
        resume_byte_pos = temp_path.stat().st_size
        LOGGER.info(f"Resuming download from byte {resume_byte_pos:,}")
    
    headers = {}
    if resume_byte_pos > 0:
        headers["Range"] = f"bytes={resume_byte_pos}-"
    
    start_time = datetime.now()
    downloaded_bytes = 0
    
    try:
        response = session.get(url, headers=headers, stream=True, timeout=DOWNLOAD_TIMEOUT)
        
        # Check if resume is supported
        if resume_byte_pos > 0 and response.status_code not in [206, 200]:
            LOGGER.warning(f"Server does not support resume (HTTP {response.status_code}), restarting download")
            temp_path.unlink()
            resume_byte_pos = 0
            response = session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)
        
        response.raise_for_status()
        
        # Get total size
        total_size = int(response.headers.get('content-length', 0))
        if resume_byte_pos > 0:
            total_size += resume_byte_pos
        
        # Download
        mode = "ab" if resume_byte_pos > 0 else "wb"
        with open(temp_path, mode) as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    downloaded_bytes += len(chunk)
                    
                    # Log progress every 100 MB (Airflow-friendly, not too noisy)
                    if downloaded_bytes % (100 * 1024 * 1024) == 0:
                        progress_mb = (resume_byte_pos + downloaded_bytes) / (1024 * 1024)
                        total_mb = total_size / (1024 * 1024) if total_size > 0 else 0
                        LOGGER.info(f"Downloaded: {progress_mb:.1f} MB / {total_mb:.1f} MB")
        
        # Atomic rename (partial → final)
        if output_path.exists():
            LOGGER.info(f"Removing existing file: {output_path}")
            output_path.unlink()
        
        temp_path.rename(output_path)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        final_size = output_path.stat().st_size
        
        return {
            "size_bytes": final_size,
            "size_gb": final_size / (1024**3),
            "duration_seconds": elapsed,
            "resumed": resume_byte_pos > 0,
            "resume_from_bytes": resume_byte_pos
        }
        
    except Exception as e:
        LOGGER.error(f"Download failed: {e}")
        # Keep partial file for resume
        if temp_path.exists():
            LOGGER.info(f"Partial file preserved for resume: {temp_path}")
        raise


# ============================================================================
# MAIN DOWNLOAD
# ============================================================================
def download_dump(config_path: str = "wikipedia.yaml") -> Dict[str, Any]:
    """
    Download latest Nepali Wikipedia dump.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Download result metadata:
        {
            "status": "downloaded" | "reused" | "skipped",
            "path": str (absolute path),
            "size_gb": float,
            "duration_seconds": float,
            "url": str
        }
        
    Raises:
        KeyError: If config missing required fields
        ValueError: If config values invalid
        requests.RequestException: On download failure
    """
    config = load_config(config_path)
    
    # Validate config early
    validate_config(config)
    
    # Extract config
    dump_dir = RAW_DIR / config["paths"]["raw_dump_dir"]
    dump_dir.mkdir(parents=True, exist_ok=True)
    
    dump_filename = config["download"]["dump_file"]
    dump_path = dump_dir / dump_filename
    
    base_url = config["download"]["base_url"]
    dump_url = urljoin(base_url, dump_filename)
    
    overwrite = config["download"].get("overwrite_existing", True)
    
    result = {
        "status": None,
        "path": str(dump_path.resolve()),
        "size_gb": 0.0,
        "duration_seconds": 0.0,
        "url": dump_url
    }
    
    LOGGER.info("="*80)
    LOGGER.info("NEPALI WIKIPEDIA DUMP DOWNLOAD")
    LOGGER.info("="*80)
    LOGGER.info(f"URL: {dump_url}")
    LOGGER.info(f"Output: {dump_path.resolve()}")
    LOGGER.info(f"Overwrite existing: {overwrite}")
    
    # Check if already exists
    if dump_path.exists():
        existing_size = dump_path.stat().st_size
        existing_size_gb = existing_size / (1024**3)
        
        if not overwrite:
            LOGGER.info(f"Dump already exists ({existing_size_gb:.2f} GB) - skipping download")
            result["status"] = "reused"
            result["size_gb"] = existing_size_gb
            return result
        else:
            LOGGER.info(f"Existing dump found ({existing_size_gb:.2f} GB) - will overwrite")
    
    # Download
    LOGGER.info("Starting download...")
    start_time = datetime.now()
    
    session = create_robust_session()
    
    try:
        download_meta = download_with_resume(dump_url, dump_path, session)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        result["status"] = "downloaded"
        result["size_gb"] = download_meta["size_gb"]
        result["duration_seconds"] = elapsed
        
        LOGGER.info("="*80)
        LOGGER.info("DOWNLOAD COMPLETE")
        LOGGER.info(f"Size: {download_meta['size_gb']:.2f} GB")
        LOGGER.info(f"Duration: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        if download_meta["resumed"]:
            LOGGER.info(f"Resumed from: {download_meta['resume_from_bytes']:,} bytes")
        LOGGER.info(f"Path: {dump_path.resolve()}")
        LOGGER.info("="*80)
        
        return result
        
    except requests.RequestException as e:
        LOGGER.error(f"Download failed after retries: {e}")
        result["status"] = "failed"
        raise
    
    except Exception as e:
        LOGGER.exception(f"Unexpected error during download: {e}")
        result["status"] = "failed"
        raise
    
    finally:
        session.close()


# ============================================================================
# MAIN
# ============================================================================
def main():
    """Main execution."""
    try:
        result = download_dump()
        LOGGER.info(f"Download result: {result['status']}")
        
        if result["status"] == "failed":
            raise RuntimeError("Download failed")
            
    except Exception as e:
        LOGGER.error(f"Failed to download Wikipedia dump: {e}")
        raise


if __name__ == "__main__":
    main()