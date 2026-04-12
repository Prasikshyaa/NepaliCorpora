from pathlib import Path

# Project root (nepali-corpus/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# -------------------------
# Data directories
# -------------------------
DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DEDUP_DIR = DATA_DIR / "deduplicated"
METADATA_DIR = DATA_DIR / "metadata"

# Raw subfolders
RAW_READY_MADE = RAW_DIR / "ready_made"
RAW_WIKIPEDIA = RAW_DIR / "wikipedia"
RAW_NEWS = RAW_DIR / "news"



# Processed subfolders
CLEANED_DIR = PROCESSED_DIR / "cleaned"
LANG_FILTERED_DIR = PROCESSED_DIR / "language_filtered"
SEGMENTED_DIR = PROCESSED_DIR / "segmented"

# Deduplicated subfolders
DEDUP_DOC = DEDUP_DIR / "document_level"
DEDUP_PARA = DEDUP_DIR / "paragraph_level"
DEDUP_SENT = DEDUP_DIR / "sentence_level"

# Logs
LOGS_ROOT = PROJECT_ROOT / "logs"
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_INGESTION = LOGS_DIR / "ingestion"
LOGS_PREPROCESSING = LOGS_DIR / "preprocessing"
LOGS_DEDUP = LOGS_DIR / "deduplication"
LOGS_WIKIPEDIA = LOGS_DIR / "wikipedia"
LOGS_AUTOMATION = LOGS_ROOT / "automation"


# Configs
CONFIGS_DIR = PROJECT_ROOT / "configs"

def create_all_dirs():
    """Create all required directories if they don't exist."""
    dirs = [
        RAW_READY_MADE, RAW_WIKIPEDIA, RAW_NEWS, 
        CLEANED_DIR, LANG_FILTERED_DIR, SEGMENTED_DIR,
        DEDUP_DOC, DEDUP_PARA, DEDUP_SENT,
        METADATA_DIR,
        LOGS_INGESTION, LOGS_PREPROCESSING, LOGS_DEDUP, LOGS_WIKIPEDIA
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
