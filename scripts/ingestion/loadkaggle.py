# scripts/ingestion/load_kaggle.py
"""
Production-ready Kaggle dataset loader for Nepali corpus.
- Reads datasets from sources.yaml
- Downloads files/folders/archives
- Converts to Parquet with streaming support for large files
- Deduplicates while loading
- Resumeable (checkpoint system)
- Outputs to data/raw/ready_made/kaggle
"""

import hashlib
import json
import shutil
import sqlite3
import tarfile
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Iterator

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

from scripts.utils.paths import RAW_DIR, METADATA_DIR
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# -------------------------------
# Paths
# -------------------------------
RAW_KAGGLE_DIR = RAW_DIR / "ready_made" / "kaggle"
RAW_KAGGLE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = METADATA_DIR / "kaggle_ingestion.db"
CHECKPOINT_FILE = METADATA_DIR / "kaggle_ingestion_checkpoint.json"
TEMP_DIR = METADATA_DIR / "tmp_kaggle"
STATS_DIR = METADATA_DIR / "kaggle_stats"
STATS_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Logger
# -------------------------------
LOGGER = get_logger("load_kaggle", log_type="ingestion")

# -------------------------------
# Config
# -------------------------------
CONFIG = load_config("preprocessing.yaml")
BATCH_SIZE = CONFIG.get("dedup_batch_size", 5000)
STREAM_CHUNK_SIZE = 100000  # Process large files in 100K row chunks

# -------------------------------
# Database
# -------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_hashes (
            dataset_name TEXT,
            text_hash TEXT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dataset ON seen_hashes(dataset_name)")
    conn.commit()
    LOGGER.info(f"Hash database initialized at {DB_PATH}")
    return conn, cur

# -------------------------------
# Checkpoint
# -------------------------------
def load_checkpoint() -> Dict:
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"processed_datasets": []}

def save_checkpoint(state: Dict):
    CHECKPOINT_FILE.write_text(json.dumps(state, indent=2))
    LOGGER.debug(f"Checkpoint saved: {len(state['processed_datasets'])} datasets processed")

# -------------------------------
# Utils
# -------------------------------
def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def authenticate_kaggle() -> KaggleApi:
    api = KaggleApi()
    api.authenticate()
    LOGGER.info("Kaggle API authenticated")
    return api

def save_stats(dataset_name: str, stats: Dict[str, Any]):
    stats_file = STATS_DIR / f"{dataset_name.replace('/', '_')}_stats.json"
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    LOGGER.info(f"Stats saved to {stats_file}")

def is_archive(path: Path) -> bool:
    """Check if file is an archive by checking full name, not just suffix."""
    name_lower = path.name.lower()
    archive_extensions = [
        '.zip', '.tar', '.tar.gz', '.tgz', 
        '.tar.bz2', '.tbz2', '.tar.xz', '.txz',
        '.tar.zst', '.7z', '.rar'
    ]
    return any(name_lower.endswith(ext) for ext in archive_extensions)

# -------------------------------
# File loading
# -------------------------------
def extract_archive(path: Path, extract_to: Path):
    """Extract archive with proper format detection."""
    extract_to.mkdir(parents=True, exist_ok=True)
    name_lower = path.name.lower()
    
    if name_lower.endswith('.zip'):
        with zipfile.ZipFile(path, 'r') as z:
            z.extractall(extract_to)
    elif any(name_lower.endswith(ext) for ext in ['.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz']):
        with tarfile.open(path, 'r:*') as t:
            t.extractall(extract_to)
    else:
        raise ValueError(f"Unsupported archive format: {path.name}")
    
    LOGGER.info(f"Extracted {path.name} to {extract_to}")

def load_text_files_from_dir(directory: Path) -> Iterator[str]:
    """
    Generator that yields text lines from all .txt files in directory.
    Memory-efficient for large directories.
    """
    for p in directory.rglob("*.txt"):
        if p.name.lower() in ["readme.md", "readme.txt"]:
            continue
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield line
        except Exception as e:
            LOGGER.warning(f"Failed to read {p}: {e}")

def load_file_streaming(path: Path, ds_info: Dict[str, Any]) -> Iterator[str]:
    """
    Stream text from files in chunks for memory efficiency.
    Yields individual text strings.
    """
    suffix = path.suffix.lower()

    # Plain text single file
    if suffix == ".txt" or ds_info.get("format") == "text":
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line
        return

    # Structured files - need to load in chunks
    text_col = ds_info.get("text_column")
    if not text_col:
        raise ValueError(f"Dataset {ds_info['name']} requires 'text_column' for structured files")
    
    # CSV streaming
    if suffix == ".csv":
        for chunk in pd.read_csv(path, encoding="utf-8", chunksize=STREAM_CHUNK_SIZE):
            if text_col not in chunk.columns:
                raise ValueError(f"{text_col} not in CSV columns {chunk.columns.tolist()}")
            for text in chunk[text_col].dropna():
                text = str(text).strip()
                if text:
                    yield text
    
    # JSONL streaming (line-by-line)
    elif suffix == ".jsonl":
        for chunk in pd.read_json(path, lines=True, chunksize=STREAM_CHUNK_SIZE):
            if text_col not in chunk.columns:
                raise ValueError(f"{text_col} not in JSONL columns {chunk.columns.tolist()}")
            for text in chunk[text_col].dropna():
                text = str(text).strip()
                if text:
                    yield text
    
    # JSON - need to load fully (can't stream)
    elif suffix == ".json":
        df = pd.read_json(path)
        if text_col not in df.columns:
            raise ValueError(f"{text_col} not in JSON columns {df.columns.tolist()}")
        for text in df[text_col].dropna():
            text = str(text).strip()
            if text:
                yield text
    
    # Parquet streaming
    elif suffix == ".parquet":
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(batch_size=STREAM_CHUNK_SIZE):
            df_chunk = batch.to_pandas()
            if text_col not in df_chunk.columns:
                raise ValueError(f"{text_col} not in Parquet columns {df_chunk.columns.tolist()}")
            for text in df_chunk[text_col].dropna():
                text = str(text).strip()
                if text:
                    yield text
    
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

# -------------------------------
# Dataset ingestion with streaming
# -------------------------------
def ingest_dataset(ds_info: Dict[str, Any], api: KaggleApi, conn, cur) -> bool:
    name = ds_info["name"]
    target_file = ds_info.get("file")
    ds_id = name.replace("/", "_")
    out_dir = RAW_KAGGLE_DIR / ds_id
    out_dir.mkdir(parents=True, exist_ok=True)
    start_time = datetime.now()

    LOGGER.info("="*80)
    LOGGER.info(f"Processing: {name}")
    LOGGER.info("="*80)

    # Check existing hashes
    cur.execute("SELECT COUNT(*) FROM seen_hashes WHERE dataset_name=?", (ds_id,))
    existing_hashes = cur.fetchone()[0]
    LOGGER.info(f"Existing hashes: {existing_hashes:,}")

    # Download
    tmp_dir = TEMP_DIR / ds_id
    tmp_dir.mkdir(parents=True, exist_ok=True)
    
    LOGGER.info("Downloading from Kaggle...")
    download_start = datetime.now()
    api.dataset_download_files(name, path=str(tmp_dir), unzip=True)
    download_time = (datetime.now() - download_start).total_seconds()
    LOGGER.info(f"Downloaded in {download_time:.1f}s")

    # Determine files to load
    file_paths: List[Path] = []

    if target_file:
        target_path = tmp_dir / target_file
        if not target_path.exists():
            available = [p.name for p in tmp_dir.iterdir()]
            raise FileNotFoundError(f"{target_file} not found. Available: {available}")
        
        if target_path.is_file():
            if is_archive(target_path):
                extract_dir = tmp_dir / "extracted"
                extract_archive(target_path, extract_dir)
                file_paths.append(extract_dir)
            else:
                file_paths.append(target_path)
        elif target_path.is_dir():
            file_paths.append(target_path)
    else:
        # Load all files in root
        file_paths.append(tmp_dir)

    # Stream and process text
    LOGGER.info("Streaming and processing text...")
    total_rows = 0
    after_clean = 0
    duplicates_found = 0
    output_batch = []
    batch_idx = 1
    hash_buffer = []
    HASH_CHECK_BATCH = 900

    for path in file_paths:
        # Get text stream
        if path.is_dir():
            text_stream = load_text_files_from_dir(path)
        else:
            text_stream = load_file_streaming(path, ds_info)
        
        # Process stream
        for text in text_stream:
            total_rows += 1
            
            # Progress logging
            if total_rows % 100000 == 0:
                LOGGER.info(f"  Processed {total_rows:,} rows...")
            
            # Skip empty
            if not text:
                continue
            
            after_clean += 1
            
            # Compute hash
            text_hash = sha256(text)
            
            # Batch check for duplicates
            hash_buffer.append((text, text_hash))
            
            if len(hash_buffer) >= HASH_CHECK_BATCH:
                # Check hashes in batch
                hashes_to_check = [h for _, h in hash_buffer]
                placeholders = ",".join("?" * len(hashes_to_check))
                cur.execute(
                    f"SELECT text_hash FROM seen_hashes WHERE text_hash IN ({placeholders})",
                    hashes_to_check
                )
                existing = {row[0] for row in cur.fetchall()}
                
                # Filter unique
                for txt, h in hash_buffer:
                    if h in existing:
                        duplicates_found += 1
                    else:
                        output_batch.append({
                            "text": txt,
                            "dataset_name": ds_id,
                            "source": "kaggle"
                        })
                        
                        # Insert hash
                        cur.execute(
                            "INSERT OR IGNORE INTO seen_hashes(dataset_name, text_hash) VALUES (?, ?)",
                            (ds_id, h)
                        )
                
                hash_buffer.clear()
                conn.commit()
                
                # Write output batch if full
                if len(output_batch) >= BATCH_SIZE:
                    df_batch = pd.DataFrame(output_batch)
                    out_file = out_dir / f"train_{batch_idx:04d}.parquet"
                    df_batch.to_parquet(out_file, index=False, engine="pyarrow")
                    LOGGER.info(f"  ✓ Batch {batch_idx}: {len(df_batch):,} rows → {out_file.name}")
                    batch_idx += 1
                    output_batch.clear()
    
    # Process remaining hash buffer
    if hash_buffer:
        hashes_to_check = [h for _, h in hash_buffer]
        placeholders = ",".join("?" * len(hashes_to_check))
        cur.execute(
            f"SELECT text_hash FROM seen_hashes WHERE text_hash IN ({placeholders})",
            hashes_to_check
        )
        existing = {row[0] for row in cur.fetchall()}
        
        for txt, h in hash_buffer:
            if h in existing:
                duplicates_found += 1
            else:
                output_batch.append({
                    "text": txt,
                    "dataset_name": ds_id,
                    "source": "kaggle"
                })
                cur.execute(
                    "INSERT OR IGNORE INTO seen_hashes(dataset_name, text_hash) VALUES (?, ?)",
                    (ds_id, h)
                )
        
        conn.commit()
    
    # Write remaining output batch
    if output_batch:
        df_batch = pd.DataFrame(output_batch)
        out_file = out_dir / f"train_{batch_idx:04d}.parquet"
        df_batch.to_parquet(out_file, index=False, engine="pyarrow")
        LOGGER.info(f"  ✓ Final batch {batch_idx}: {len(df_batch):,} rows → {out_file.name}")
        batch_idx += 1

    new_unique = after_clean - duplicates_found
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Save statistics
    save_stats(ds_id, {
        "dataset_name": name,
        "timestamp": datetime.now().isoformat(),
        "input_rows": total_rows,
        "after_cleaning": after_clean,
        "duplicates": duplicates_found,
        "new_unique": new_unique,
        "batches_written": batch_idx - 1,
        "processing_time_seconds": round(elapsed, 2),
        "output_directory": str(out_dir)
    })

    # Cleanup
    shutil.rmtree(tmp_dir, ignore_errors=True)
    
    LOGGER.info("="*80)
    LOGGER.info(f"COMPLETE: {name}")
    LOGGER.info(f"Total rows: {total_rows:,}")
    LOGGER.info(f"After cleaning: {after_clean:,}")
    LOGGER.info(f"Duplicates: {duplicates_found:,}")
    LOGGER.info(f"New unique: {new_unique:,}")
    LOGGER.info(f"Batches: {batch_idx - 1}")
    LOGGER.info(f"Time: {elapsed:.1f}s")
    LOGGER.info("="*80)
    
    return True

# -------------------------------
# Main
# -------------------------------
def main():
    sources = load_config("sources.yaml").get("kaggle_datasets", [])
    if not sources:
        LOGGER.warning("No Kaggle datasets in sources.yaml")
        return

    checkpoint = load_checkpoint()
    api = authenticate_kaggle()
    conn, cur = init_db()
    successful, failed, skipped = 0, 0, 0
    overall_start = datetime.now()

    LOGGER.info("="*80)
    LOGGER.info("KAGGLE DATASET INGESTION")
    LOGGER.info(f"Datasets: {len(sources)}")
    LOGGER.info("="*80)

    for idx, ds in enumerate(sources, 1):
        name = ds["name"]
        LOGGER.info(f"\n[{idx}/{len(sources)}] Starting: {name}")
        
        if name in checkpoint["processed_datasets"]:
            LOGGER.info(f"Skipping (already processed)")
            skipped += 1
            continue
        
        try:
            result = ingest_dataset(ds, api, conn, cur)
            if result:
                successful += 1
                checkpoint["processed_datasets"].append(name)
                save_checkpoint(checkpoint)
            else:
                failed += 1
        except Exception as e:
            LOGGER.exception(f"FAILED: {name}")
            failed += 1
            continue

    overall_elapsed = (datetime.now() - overall_start).total_seconds()
    conn.close()

    LOGGER.info("")
    LOGGER.info("="*80)
    LOGGER.info("KAGGLE INGESTION COMPLETE")
    LOGGER.info(f"Successful: {successful}")
    LOGGER.info(f"Failed: {failed}")
    LOGGER.info(f"Skipped: {skipped}")
    LOGGER.info(f"Total time: {overall_elapsed:.1f}s")
    LOGGER.info(f"Database: {DB_PATH}")
    LOGGER.info(f"Stats: {STATS_DIR}")
    LOGGER.info("="*80)

if __name__ == "__main__":
    main()