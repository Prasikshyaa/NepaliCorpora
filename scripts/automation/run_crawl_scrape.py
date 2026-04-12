#!/usr/bin/env python3
"""
Automation script for sequential crawl → scrape pipeline.

This script orchestrates the full news site data collection pipeline:
1. Crawl all sites in websites.yaml to discover article URLs
2. Scrape discovered URLs to extract article content

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

# Script timeout in seconds (2 hours default)
SCRIPT_TIMEOUT = 7200

# Module names to run (using -m flag for proper package execution)
WEBCRAWLER_MODULE = "scripts.ingestion.webcrawler"
WEBSCRAPER_MODULE = "scripts.ingestion.webscraper"

LOGGER = get_logger("automation.crawl_scrape", log_type="automation")

# ============================================================================
# INTERRUPT HANDLING
# ============================================================================

class InterruptHandler:
    """
    Handles Ctrl+C interrupts with skip vs exit logic.
    
    - First Ctrl+C: Skip current step
    - Second Ctrl+C (within 3 seconds): Exit immediately
    """
    
    def __init__(self):
        self.interrupt_count = 0
        self.last_interrupt_time = 0
        self.interrupt_window = 3.0  # seconds
        self.current_process = None
        
    def reset(self):
        """Reset interrupt counter (call at start of each step)."""
        self.interrupt_count = 0
        self.last_interrupt_time = 0
        
    def handle_interrupt(self, signum, frame):
        """Handle SIGINT (Ctrl+C) signal."""
        current_time = time.time()
        
        # Check if this is a second interrupt within the window
        if current_time - self.last_interrupt_time < self.interrupt_window:
            self.interrupt_count += 1
            LOGGER.warning("\n" + "!"*80)
            LOGGER.warning("SECOND Ctrl+C DETECTED - EXITING PIPELINE IMMEDIATELY")
            LOGGER.warning("!"*80)
            
            # Kill current subprocess if running
            if self.current_process:
                try:
                    self.current_process.terminate()
                except:
                    pass
            
            sys.exit(1)
        else:
            # First interrupt or outside window
            self.interrupt_count = 1
            self.last_interrupt_time = current_time
            
            LOGGER.warning("\n" + "!"*80)
            LOGGER.warning("Ctrl+C DETECTED - SKIPPING CURRENT STEP")
            LOGGER.warning("Press Ctrl+C again within 3 seconds to EXIT entire pipeline")
            LOGGER.warning("!"*80)
            
            # Terminate current subprocess
            if self.current_process:
                try:
                    LOGGER.info("Terminating current subprocess...")
                    self.current_process.terminate()
                    
                    # Give it a moment to terminate gracefully
                    try:
                        self.current_process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        LOGGER.warning("Subprocess didn't terminate gracefully, killing it...")
                        self.current_process.kill()
                except Exception as e:
                    LOGGER.warning(f"Error terminating subprocess: {e}")
            
            # Raise KeyboardInterrupt to break out of subprocess.run()
            raise KeyboardInterrupt("User requested skip")

# Global interrupt handler
interrupt_handler = InterruptHandler()

# ============================================================================
# SUBPROCESS EXECUTION
# ============================================================================

def run_module(module_name: str, script_name: str, timeout: int = SCRIPT_TIMEOUT) -> tuple[bool, bool]:
    """
    Run a Python module using -m flag and capture output.
    
    Args:
        module_name: Module name (e.g., "scripts.ingestion.webcrawler")
        script_name: Human-readable name for logging
        timeout: Timeout in seconds (default: 2 hours)
        
    Returns:
        Tuple of (success: bool, was_skipped: bool)
        - (True, False) = Completed successfully
        - (False, False) = Failed
        - (False, True) = Skipped by user
    """
    # Reset interrupt counter at start of each step
    interrupt_handler.reset()
    
    LOGGER.info("="*80)
    LOGGER.info(f"Running: {script_name}")
    LOGGER.info(f"Module: {module_name}")
    LOGGER.info(f"Timeout: {timeout}s ({timeout/3600:.1f} hours)")
    LOGGER.info(f"Tip: Press Ctrl+C once to skip, twice to exit completely")
    LOGGER.info("="*80)
    
    start_time = time.time()
    
    try:
        # Build command: python -m module_name
        cmd = [sys.executable, "-m", module_name]
        
        # Platform-specific subprocess flags
        kwargs = {
            "cwd": PROJECT_ROOT,
            "capture_output": True,
            "text": True,
            "timeout": timeout,
        }
        
        # On Windows, use CREATE_NEW_PROCESS_GROUP for better signal handling
        if platform.system() == "Windows":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        
        LOGGER.info(f"Executing: {' '.join(cmd)}")
        LOGGER.info(f"Working directory: {PROJECT_ROOT}")
        
        # Create subprocess using Popen so we can store reference
        process = subprocess.Popen(
            cmd,
            cwd=kwargs["cwd"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        
        # Store process reference for interrupt handler
        interrupt_handler.current_process = process
        
        # Wait for process with timeout
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            returncode = process.returncode
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            elapsed = time.time() - start_time
            LOGGER.error(f"✗ {script_name} timed out after {elapsed:.1f}s")
            LOGGER.error(f"  Timeout limit: {timeout}s ({timeout/3600:.1f} hours)")
            interrupt_handler.current_process = None
            return False, False
        finally:
            interrupt_handler.current_process = None
        
        elapsed = time.time() - start_time
        
        # Log stdout line by line
        if stdout:
            LOGGER.info(f"--- {script_name} Output ---")
            for line in stdout.strip().split('\n'):
                if line.strip():  # Skip empty lines
                    LOGGER.info(f"  {line}")
        
        # Log stderr line by line
        if stderr:
            LOGGER.warning(f"--- {script_name} Errors/Warnings ---")
            for line in stderr.strip().split('\n'):
                if line.strip():  # Skip empty lines
                    LOGGER.warning(f"  {line}")
        
        # Check exit code
        if returncode == 0:
            LOGGER.info(f"✓ {script_name} completed successfully in {elapsed:.1f}s")
            return True, False
        else:
            LOGGER.error(f"✗ {script_name} failed with exit code {returncode}")
            LOGGER.error(f"  Elapsed time: {elapsed:.1f}s")
            return False, False
    
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        LOGGER.warning(f"⊘ {script_name} skipped by user after {elapsed:.1f}s")
        interrupt_handler.current_process = None
        return False, True  # Return skipped=True
    
    except FileNotFoundError as e:
        LOGGER.error(f"✗ {script_name} failed: Python executable not found")
        LOGGER.error(f"  Error: {e}")
        interrupt_handler.current_process = None
        return False, False
    
    except Exception as e:
        elapsed = time.time() - start_time
        LOGGER.exception(f"✗ {script_name} failed with exception after {elapsed:.1f}s: {e}")
        interrupt_handler.current_process = None
        return False, False

# ============================================================================
# MAIN PIPELINE
# ============================================================================

# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_crawl_scrape_pipeline(mode: str = "both") -> int:
    """
    Run the crawl → scrape pipeline with selectable modes.

    Args:
        mode: "crawl", "scrape", or "both"

    Returns:
        Exit code (0=success, 1=failure)
    """
    LOGGER.info("="*80)
    LOGGER.info("CRAWL & SCRAPE AUTOMATION PIPELINE")
    LOGGER.info(f"Mode: {mode}")
    LOGGER.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    LOGGER.info(f"Platform: {platform.system()} {platform.release()}")
    LOGGER.info(f"Python: {sys.version}")
    LOGGER.info(f"Project root: {PROJECT_ROOT}")
    LOGGER.info("")
    LOGGER.info("Interrupt Controls:")
    LOGGER.info("  • Press Ctrl+C ONCE to skip current step")
    LOGGER.info("  • Press Ctrl+C TWICE (within 3s) to exit completely")
    LOGGER.info("="*80)

    pipeline_start = time.time()
    steps_completed = []
    steps_skipped = []
    steps_failed = []

    # Determine which steps to run
    run_crawl = mode in ["both", "crawl"]
    run_scrape = mode in ["both", "scrape"]

    # Step 1: Crawl all sites (if requested)
    if run_crawl:
        LOGGER.info("\n[STEP 1] Running web crawler...")
        LOGGER.info("This step discovers article URLs from configured news sites")

        crawl_success, crawl_skipped = run_module(WEBCRAWLER_MODULE, "Web Crawler")

        if crawl_skipped:
            LOGGER.warning("\n⊘ Crawling skipped - continuing to next steps")
            steps_skipped.append("Web Crawler")
        elif not crawl_success:
            LOGGER.error("\n✗ Crawling failed - aborting pipeline")
            steps_failed.append("Web Crawler")
            return 1
        else:
            LOGGER.info("\n✓ Crawling completed successfully")
            steps_completed.append("Web Crawler")

    # Step 2: Scrape discovered articles (if requested)
    if run_scrape:
        step_num = 2 if run_crawl else 1
        total_steps = (1 if run_crawl else 0) + (1 if run_scrape else 0)

        LOGGER.info(f"\n[STEP {step_num}/{total_steps}] Running web scraper...")
        LOGGER.info("This step extracts article content from discovered URLs")

        scrape_success, scrape_skipped = run_module(WEBSCRAPER_MODULE, "Web Scraper")

        if scrape_skipped:
            LOGGER.warning("\n⊘ Scraping skipped - pipeline incomplete")
            steps_skipped.append("Web Scraper")
        elif not scrape_success:
            LOGGER.error("\n✗ Scraping failed - pipeline incomplete")
            steps_failed.append("Web Scraper")
            return 1
        else:
            LOGGER.info("\n✓ Scraping completed successfully")
            steps_completed.append("Web Scraper")

    # Summary
    pipeline_elapsed = time.time() - pipeline_start

    LOGGER.info("\n" + "="*80)
    LOGGER.info("PIPELINE SUMMARY")
    LOGGER.info("="*80)

    if steps_completed:
        LOGGER.info(f"✓ Completed: {', '.join(steps_completed)}")
    if steps_skipped:
        LOGGER.warning(f"⊘ Skipped:   {', '.join(steps_skipped)}")
    if steps_failed:
        LOGGER.error(f"✗ Failed:    {', '.join(steps_failed)}")

    LOGGER.info("")
    LOGGER.info(f"Total time: {pipeline_elapsed/60:.1f} minutes ({pipeline_elapsed:.1f}s)")
    LOGGER.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Determine exit code
    if steps_failed:
        LOGGER.error("\nPIPELINE FAILED ✗")
        LOGGER.info("="*80)
        return 1
    elif steps_skipped and not steps_completed:
        LOGGER.warning("\nPIPELINE INCOMPLETE (all steps skipped) ⊘")
        LOGGER.info("="*80)
        return 1
    elif steps_skipped:
        LOGGER.warning("\nPIPELINE PARTIALLY COMPLETE ⊘")
        LOGGER.info("="*80)
        return 0  # Partial success
    else:
        LOGGER.info("\nPIPELINE COMPLETE ✓")
        LOGGER.info("="*80)
        return 0

# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point with comprehensive error handling."""

    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Nepali Corpus Crawl & Scrape Automation")
    parser.add_argument(
        'mode',
        nargs='?',
        default='both',
        choices=['crawl', 'scrape', 'both'],
        help='Pipeline mode: crawl, scrape, or both (default: both)'
    )
    args = parser.parse_args()

    # Set up signal handler for Ctrl+C
    signal.signal(signal.SIGINT, interrupt_handler.handle_interrupt)

    try:
        # Verify project structure
        if not PROJECT_ROOT.exists():
            LOGGER.error(f"Project root does not exist: {PROJECT_ROOT}")
            sys.exit(1)

        # Verify scripts exist (as modules)
        webcrawler_path = PROJECT_ROOT / "scripts" / "ingestion" / "webcrawler.py"
        webscraper_path = PROJECT_ROOT / "scripts" / "ingestion" / "webscraper.py"

        if not webcrawler_path.exists():
            LOGGER.error(f"Web crawler script not found: {webcrawler_path}")
            LOGGER.error("Please ensure the scripts/ingestion/webcrawler.py file exists")
            sys.exit(1)

        if not webscraper_path.exists():
            LOGGER.error(f"Web scraper script not found: {webscraper_path}")
            LOGGER.error("Please ensure the scripts/ingestion/webscraper.py file exists")
            sys.exit(1)

        # Run the pipeline
        exit_code = run_crawl_scrape_pipeline(args.mode)
        sys.exit(exit_code)

    except KeyboardInterrupt:
        # This catches the second Ctrl+C from signal handler
        LOGGER.warning("\n\n" + "="*80)
        LOGGER.warning("PIPELINE INTERRUPTED BY USER")
        LOGGER.warning("="*80)
        sys.exit(1)

    except Exception as e:
        LOGGER.exception(f"\n\nPIPELINE FAILED WITH UNEXPECTED ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()