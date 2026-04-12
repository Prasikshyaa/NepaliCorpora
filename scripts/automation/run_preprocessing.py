# scripts/automation/run_preprocessing.py
"""
Automation script for corpus preprocessing pipeline.

This script orchestrates the full preprocessing pipeline:
1. Text cleaning (Unicode normalization, HTML removal, etc.)
2. Language filtering (ensure Nepali content)
3. Text segmentation (sentence splitting)

Interrupt Handling:
- Press Ctrl+C ONCE: Skip current step and continue to next step
- Press Ctrl+C TWICE: Exit entire pipeline immediately

Designed for Airflow execution with:
- Proper exit codes (0=success, 1=failure)
- Comprehensive logging to console and files
- Error handling with partial failure recovery
- Progress tracking
- Cross-platform compatibility (Windows/Linux/Mac)
"""

import sys
import time
import subprocess
import platform
import signal
from pathlib import Path
from datetime import datetime

# Add project root to path BEFORE any other imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.utils.logger import get_logger

# ============================================================================
# CONFIGURATION
# ============================================================================

# Script timeout in seconds (2 hours default for preprocessing)
SCRIPT_TIMEOUT = 7200

# Module names to run (using -m flag for proper package execution)
CLEAN_TEXT_MODULE = "scripts.preprocessing.clean_text"
LANGUAGE_FILTER_MODULE = "scripts.preprocessing.language_filter"
SEGMENT_TEXT_MODULE = "scripts.preprocessing.segment_text"
EXACT_DEDUP_MODULE = "scripts.deduplication.exact_dedup"
NEAR_DEDUP_MODULE = "scripts.deduplication.near_dedup"

LOGGER = get_logger("automation.preprocessing", log_type="automation")

# ============================================================================
# INTERRUPT HANDLING
# ============================================================================

interrupt_count = 0
original_sigint_handler = None

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global interrupt_count

    interrupt_count += 1
    LOGGER.warning(f"⚠️  Interrupt signal received (count: {interrupt_count})")

    if interrupt_count == 1:
        LOGGER.warning("⏭️  Skipping current step and continuing to next...")
        # Don't exit, just continue to next step
    elif interrupt_count >= 2:
        LOGGER.error("🛑 Force exit requested. Terminating pipeline.")
        sys.exit(1)

def setup_signal_handlers():
    """Setup signal handlers for graceful interruption."""
    global original_sigint_handler

    if platform.system() == "Windows":
        # Windows doesn't support SIGUSR1, use SIGINT only
        original_sigint_handler = signal.signal(signal.SIGINT, signal_handler)
    else:
        # Unix-like systems
        original_sigint_handler = signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGUSR1, signal_handler)

def restore_signal_handlers():
    """Restore original signal handlers."""
    if original_sigint_handler:
        signal.signal(signal.SIGINT, original_sigint_handler)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def run_python_module(module_name: str, description: str, timeout: int = SCRIPT_TIMEOUT) -> bool:
    """
    Run a Python module with proper error handling and timeout.

    Args:
        module_name: Module to run (e.g., 'scripts.preprocessing.clean_text')
        description: Human-readable description for logging
        timeout: Timeout in seconds

    Returns:
        bool: True if successful, False otherwise
    """
    global interrupt_count

    LOGGER.info("="*60)
    LOGGER.info(f"🚀 STARTING: {description}")
    LOGGER.info(f"Module: {module_name}")
    LOGGER.info("="*60)

    start_time = datetime.now()

    try:
        # Prepare command
        cmd = [sys.executable, "-m", module_name]

        LOGGER.info(f"Command: {' '.join(cmd)}")

        # Run with timeout
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            timeout=timeout,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )

        # Log output
        if result.stdout:
            LOGGER.info(f"📄 STDOUT:\n{result.stdout}")

        if result.stderr:
            if result.returncode == 0:
                LOGGER.warning(f"⚠️  STDERR:\n{result.stderr}")
            else:
                LOGGER.error(f"❌ STDERR:\n{result.stderr}")

        # Check result
        if result.returncode == 0:
            duration = datetime.now() - start_time
            LOGGER.info(f"✅ COMPLETED: {description}")
            LOGGER.info(f"⏱️  Duration: {duration}")
            return True
        else:
            LOGGER.error(f"❌ FAILED: {description} (exit code: {result.returncode})")
            return False

    except subprocess.TimeoutExpired:
        LOGGER.error(f"⏰ TIMEOUT: {description} exceeded {timeout} seconds")
        return False
    except KeyboardInterrupt:
        if interrupt_count >= 2:
            LOGGER.error(f"🛑 INTERRUPTED: {description}")
            return False
        else:
            LOGGER.warning(f"⏭️  SKIPPED: {description} (interrupt)")
            return True  # Consider skipped as successful for pipeline continuation
    except Exception as e:
        LOGGER.error(f"💥 EXCEPTION in {description}: {e}")
        return False

def check_prerequisites() -> bool:
    """
    Check if all prerequisites are met before starting preprocessing.

    Returns:
        bool: True if all prerequisites are met
    """
    LOGGER.info("🔍 Checking prerequisites...")

    # Check if scraped data exists
    processed_dir = PROJECT_ROOT / "data" / "processed" / "huggingface"
    if not processed_dir.exists():
        LOGGER.error(f"❌ Processed data directory not found: {processed_dir}")
        LOGGER.error("   Run scraping pipeline first!")
        return False

    parquet_files = list(processed_dir.rglob("*.parquet"))
    if not parquet_files:
        LOGGER.error(f"❌ No parquet files found in {processed_dir}")
        LOGGER.error("   Run scraping pipeline first!")
        return False

    LOGGER.info(f"✅ Found {len(parquet_files)} parquet files to preprocess")
    return True

# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    """Main preprocessing pipeline."""
    LOGGER.info("="*80)
    LOGGER.info("🧹 NEPALI CORPUS PREPROCESSING PIPELINE")
    LOGGER.info("Automated text cleaning, filtering, and segmentation")
    LOGGER.info("="*80)

    start_time = datetime.now()

    # Setup signal handlers
    setup_signal_handlers()

    try:
        # Check prerequisites
        if not check_prerequisites():
            LOGGER.error("❌ Prerequisites not met. Exiting.")
            return 1

        # Step 1: Text Cleaning
        success = run_python_module(
            CLEAN_TEXT_MODULE,
            "Text Cleaning (Unicode, HTML, formatting)",
            timeout=SCRIPT_TIMEOUT
        )

        if not success and interrupt_count == 0:
            LOGGER.error("❌ Text cleaning failed. Stopping pipeline.")
            return 1

        # Step 2: Language Filtering
        try:
            # Check if language filter is implemented
            lang_filter_path = PROJECT_ROOT / "scripts" / "preprocessing" / "language_filter.py"
            if lang_filter_path.exists() and lang_filter_path.stat().st_size > 1000:  # More than just empty file
                success = run_python_module(
                    LANGUAGE_FILTER_MODULE,
                    "Language Filtering (Nepali content validation)",
                    timeout=SCRIPT_TIMEOUT
                )

                if not success and interrupt_count == 0:
                    LOGGER.error("❌ Language filtering failed. Stopping pipeline.")
                    return 1
            else:
                LOGGER.info("⏭️  Language filtering not implemented yet, skipping...")
        except Exception as e:
            LOGGER.warning(f"Language filtering check failed: {e}, skipping...")

        # Step 3: Text Segmentation
        try:
            # Check if text segmentation is implemented
            segment_path = PROJECT_ROOT / "scripts" / "preprocessing" / "segment_text.py"
            if segment_path.exists() and segment_path.stat().st_size > 1000:  # More than just empty file
                success = run_python_module(
                    SEGMENT_TEXT_MODULE,
                    "Text Segmentation (sentence splitting)",
                    timeout=SCRIPT_TIMEOUT
                )

                if not success and interrupt_count == 0:
                    LOGGER.error("❌ Text segmentation failed. Stopping pipeline.")
                    return 1
            else:
                LOGGER.info("⏭️  Text segmentation not implemented yet, skipping...")
        except Exception as e:
            LOGGER.warning(f"Text segmentation check failed: {e}, skipping...")

        # Step 4: Exact Deduplication
        success = run_python_module(
            EXACT_DEDUP_MODULE,
            "Exact Deduplication (remove duplicate documents)",
            timeout=SCRIPT_TIMEOUT
        )

        if not success and interrupt_count == 0:
            LOGGER.error("❌ Exact deduplication failed. Stopping pipeline.")
            return 1

        # Step 5: Near Deduplication
        success = run_python_module(
            NEAR_DEDUP_MODULE,
            "Near Deduplication (remove similar documents)",
            timeout=SCRIPT_TIMEOUT
        )

        if not success and interrupt_count == 0:
            LOGGER.error("❌ Near deduplication failed. Stopping pipeline.")
            return 1

        # Pipeline completed
        duration = datetime.now() - start_time
        LOGGER.info("="*80)
        LOGGER.info("🎉 PREPROCESSING PIPELINE COMPLETED SUCCESSFULLY")
        LOGGER.info(f"⏱️  Total duration: {duration}")
        LOGGER.info("="*80)

        return 0

    except KeyboardInterrupt:
        LOGGER.warning("🛑 Pipeline interrupted by user")
        return 1
    except Exception as e:
        LOGGER.error(f"💥 Pipeline failed with exception: {e}")
        return 1
    finally:
        restore_signal_handlers()

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)