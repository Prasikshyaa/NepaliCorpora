#!/usr/bin/env python3
"""
Automation script for Wikipedia dump download → extraction pipeline.

This script orchestrates the Wikipedia data collection pipeline:
1. Download latest Nepali Wikipedia dump from Wikimedia
2. Extract and clean articles to processed format

Designed for Airflow execution with:
- Proper exit codes (0=success, 1=failure)
- Comprehensive logging to console and files
- Error handling with resume capability
- Progress tracking
"""

import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.logger import get_logger

LOGGER = get_logger("automation.wikipedia", log_type="automation")

# ============================================================================

# Define modules to run
MODULES = [
    ("scripts.ingestion.wikipedia.download_dump", "Wikipedia Downloader"),
    ("scripts.ingestion.wikipedia.extract_nepali", "Wikipedia Extractor"),
]

# ============================================================================

def run_module(module_name: str, friendly_name: str) -> bool:
    """Run a Python module as subprocess and log output."""
    LOGGER.info("="*80)
    LOGGER.info(f"Running: {friendly_name}")
    LOGGER.info(f"Module: {module_name}")
    LOGGER.info("="*80)

    start_time = time.time()

    try:
        result = subprocess.run(
            [sys.executable, "-m", module_name],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=14400,  # 4 hours
            start_new_session=True  # allows Ctrl+C interrupt
        )

        elapsed = time.time() - start_time

        if result.stdout:
            LOGGER.info(f"--- {friendly_name} Output ---")
            for line in result.stdout.strip().splitlines():
                LOGGER.info(line)

        if result.stderr:
            LOGGER.warning(f"--- {friendly_name} Errors ---")
            for line in result.stderr.strip().splitlines():
                LOGGER.warning(line)

        if result.returncode == 0:
            LOGGER.info(f"✓ {friendly_name} completed successfully in {elapsed:.1f}s")
            return True
        else:
            LOGGER.error(f"✗ {friendly_name} failed with exit code {result.returncode}")
            return False

    except subprocess.TimeoutExpired:
        LOGGER.error(f"✗ {friendly_name} timed out after 4 hours")
        return False
    except Exception as e:
        LOGGER.exception(f"✗ {friendly_name} failed with exception: {e}")
        return False

# ============================================================================

def run_wikipedia_pipeline() -> int:
    """Run download → extract pipeline."""
    LOGGER.info("="*80)
    LOGGER.info("WIKIPEDIA AUTOMATION PIPELINE")
    LOGGER.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    LOGGER.info("="*80)

    pipeline_start = time.time()

    for module_name, friendly_name in MODULES:
        success = run_module(module_name, friendly_name)
        if not success:
            LOGGER.error(f"{friendly_name} failed - aborting pipeline")
            return 1

    pipeline_elapsed = time.time() - pipeline_start
    LOGGER.info("\n" + "="*80)
    LOGGER.info("PIPELINE COMPLETE")
    LOGGER.info(f"Total time: {pipeline_elapsed/60:.1f} minutes")
    LOGGER.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    LOGGER.info("="*80)

    return 0

# ============================================================================

def main():
    try:
        exit_code = run_wikipedia_pipeline()
        sys.exit(exit_code)

    except KeyboardInterrupt:
        LOGGER.warning("\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        LOGGER.exception(f"Pipeline failed with unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
