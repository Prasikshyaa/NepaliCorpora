# preflight_check.py
"""Complete pre-flight check for Wikipedia pipeline."""

import sys
from pathlib import Path
import shutil

def check_directories():
    """Check required directories exist."""
    print("\n[1/7] Checking Directories...")
    
    dirs = [
        "data/raw/wikipedia/latest",
        "data/processed/wikipedia",
        "data/logs/wikipedia",
        "data/metadata",
        "configs",
        "scripts/ingestion",
        "scripts/automation",
        "scripts/utils"
    ]
    
    missing = []
    for d in dirs:
        path = Path(d)
        if path.exists():
            print(f"  ✓ {d}")
        else:
            print(f"  ✗ {d} - MISSING")
            missing.append(d)
    
    if missing:
        print(f"\n  Create with: mkdir -p {' '.join(missing)}")
        return False
    
    return True


def check_dependencies():
    """Check Python dependencies."""
    print("\n[2/7] Checking Dependencies...")
    
    deps = ["lxml", "mwparserfromhell", "requests", "pandas", "pyarrow", "yaml", "tqdm"]
    missing = []
    
    for dep in deps:
        try:
            __import__(dep)
            print(f"  ✓ {dep}")
        except ImportError:
            print(f"  ✗ {dep} - MISSING")
            missing.append(dep)
    
    if missing:
        print(f"\n  Install with: pip install {' '.join(missing)}")
        return False
    
    return True


def check_config():
    """Check config file exists."""
    print("\n[3/7] Checking Configuration...")
    
    config_path = Path("configs/wikipedia.yaml")
    
    if config_path.exists():
        print(f"  ✓ {config_path}")
        try:
            from scripts.utils.config import load_config
            config = load_config("wikipedia.yaml")
            print(f"  ✓ Config valid")
            return True
        except Exception as e:
            print(f"  ✗ Config invalid: {e}")
            return False
    else:
        print(f"  ✗ {config_path} - NOT FOUND")
        return False


def check_utils():
    """Check utility modules."""
    print("\n[4/7] Checking Utilities...")
    
    try:
        from scripts.utils.paths import RAW_DIR, PROCESSED_DIR
        print(f"  ✓ paths.py")
    except Exception as e:
        print(f"  ✗ paths.py - {e}")
        return False
    
    try:
        from scripts.utils.logger import get_logger
        print(f"  ✓ logger.py")
    except Exception as e:
        print(f"  ✗ logger.py - {e}")
        return False
    
    try:
        from scripts.utils.config import load_config
        print(f"  ✓ config.py")
    except Exception as e:
        print(f"  ✗ config.py - {e}")
        return False
    
    return True


def check_scripts():
    """Check main scripts importable."""
    print("\n[5/7] Checking Scripts...")
    
    try:
        from scripts.ingestion.wikipedia.download_dump import download_dump
        print(f"  ✓ download_dump.py")
    except Exception as e:
        print(f"  ✗ download_dump.py - {e}")
        return False
    
    try:
        from scripts.ingestion.wikipedia.extract_nepali import extract_nepali
        print(f"  ✓ extract_nepali.py")
    except Exception as e:
        print(f"  ✗ extract_nepali.py - {e}")
        return False
    
    try:
        from scripts.automation.run_wiki_pipeline import run_wikipedia_pipeline
        print(f"  ✓ run_wiki_pipeline.py")
    except Exception as e:
        print(f"  ✗ run_wiki_pipeline.py - {e}")
        return False
    
    return True


def check_network():
    """Check Wikimedia accessibility."""
    print("\n[6/7] Checking Network...")
    
    try:
        import requests
        response = requests.head("https://dumps.wikimedia.org/newwiki/latest/", timeout=10)
        
        if response.status_code == 200:
            print(f"  ✓ Wikimedia accessible")
            return True
        else:
            print(f"  ⚠️  Unexpected status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"  ✗ Network error: {e}")
        return False


def check_disk_space():
    """Check disk space."""
    print("\n[7/7] Checking Disk Space...")
    
    stat = shutil.disk_usage(".")
    free_gb = stat.free / (1024**3)
    
    print(f"  Free space: {free_gb:.2f} GB")
    
    if free_gb >= 5.0:
        print(f"  ✓ Sufficient space")
        return True
    else:
        print(f"  ⚠️  Low disk space (recommend 5+ GB)")
        return True  # Warning, not error


def main():
    """Run all checks."""
    print("="*60)
    print("WIKIPEDIA PIPELINE PRE-FLIGHT CHECK")
    print("="*60)
    
    checks = [
        check_directories,
        check_dependencies,
        check_config,
        check_utils,
        check_scripts,
        check_network,
        check_disk_space
    ]
    
    results = [check() for check in checks]
    
    print("\n" + "="*60)
    if all(results):
        print("✅ ALL CHECKS PASSED - READY TO RUN!")
        print("="*60)
        print("\nNext steps:")
        print("  1. python -m scripts.automation.run_wiki_pipeline")
        print("  2. Check logs in data/logs/wikipedia/")
        return 0
    else:
        print("❌ SOME CHECKS FAILED - FIX ISSUES ABOVE")
        print("="*60)
        return 1


if __name__ == "__main__":
    sys.exit(main())