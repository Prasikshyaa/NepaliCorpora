# scripts/ingestion/load_ready_made.py
import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
from datasets import load_dataset
from scripts.utils.paths import RAW_READY_MADE, METADATA_DIR
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# --------------------------
# Logger
# --------------------------
LOGGER = get_logger("load_ready_made", log_type="ingestion")

# --------------------------
# Database for tracking ingested text hashes
# --------------------------
DB_PATH = METADATA_DIR / "hf_ingestion.db"
LOGGER.info(f"Database path: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS seen_hashes (
        dataset_name TEXT,
        text_hash TEXT PRIMARY KEY,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
)
conn.commit()
LOGGER.info("Database initialized successfully")

# --------------------------
# Load sources.yaml
# --------------------------
config = load_config("sources.yaml")
datasets_to_load = config.get("huggingface_datasets", [])
LOGGER.info(f"Found {len(datasets_to_load)} datasets to load")

# --------------------------
# Helper function to hash text
# --------------------------
def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# --------------------------
# Main ingestion function
# --------------------------
def ingest_dataset(dataset_info: dict, batch_size: int = 5000, hash_commit_interval: int = 1000):
    name = dataset_info["name"]
    text_column = dataset_info["text_column"]

    LOGGER.info(f"="*80)
    LOGGER.info(f"Starting ingestion: {name}")
    LOGGER.info(f"Text column: {text_column}")
    LOGGER.info(f"Batch size: {batch_size}")
    LOGGER.info(f"="*80)

    out_dir = RAW_READY_MADE / name.replace("/", "_")
    out_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info(f"Output directory: {out_dir}")

    # Check existing hashes
    cur.execute("SELECT COUNT(*) FROM seen_hashes WHERE dataset_name=?", (name,))
    existing_hashes = cur.fetchone()[0]
    LOGGER.info(f"Existing hashes in DB: {existing_hashes}")

    # Load dataset streaming to avoid memory issues
    try:
        ds = load_dataset(name, split="train", streaming=True)
        LOGGER.info("Dataset loaded successfully in streaming mode")
    except Exception as e:
        LOGGER.error(f"Failed to load dataset: {e}")
        raise

    batch = []
    batch_number = 1
    total_written = 0
    total_processed = 0
    skipped_no_column = 0
    skipped_empty = 0
    skipped_duplicate = 0
    hash_insert_count = 0

    start_time = datetime.now()
    LOGGER.info(f"Starting processing at {start_time}")

    for idx, row in enumerate(ds):
        total_processed += 1
        
        # Debug: Print first row structure
        if idx == 0:
            LOGGER.info(f"First row columns: {list(row.keys())}")
            LOGGER.info(f"Sample row keys: {list(row.keys())}")
            if text_column in row:
                LOGGER.info(f"First text sample (first 100 chars): {str(row[text_column])[:100]}")
            else:
                LOGGER.error(f"COLUMN MISMATCH! '{text_column}' not found. Available: {list(row.keys())}")
        
        # Check if text_column exists
        if text_column not in row:
            skipped_no_column += 1
            if skipped_no_column == 1:
                LOGGER.warning(f"Column '{text_column}' not found. Available: {list(row.keys())}")
            continue
            
        # Check if text is empty
        if not row[text_column]:
            skipped_empty += 1
            continue

        txt = row[text_column]
        src = row.get("Source", row.get("source", None))

        # Check for duplicates
        h = text_hash(txt)
        cur.execute(
            "SELECT 1 FROM seen_hashes WHERE text_hash=? AND dataset_name=?", 
            (h, name)
        )
        if cur.fetchone():
            skipped_duplicate += 1
            continue

        # Add to batch
        batch.append({"text": txt, "source": src, "dataset_name": name})

        # Insert hash
        cur.execute(
            "INSERT OR IGNORE INTO seen_hashes(dataset_name, text_hash) VALUES (?, ?)",
            (name, h)
        )
        hash_insert_count += 1

        # Commit hashes periodically (not just on batch write)
        if hash_insert_count % hash_commit_interval == 0:
            conn.commit()
            LOGGER.debug(f"Committed {hash_insert_count} hashes to database")

        # Write batch when full
        if len(batch) >= batch_size:
            df = pd.DataFrame(batch)
            out_file = out_dir / f"train_{batch_number:04d}.parquet"
            df.to_parquet(out_file, engine="pyarrow", index=False)
            LOGGER.info(f"✓ Batch {batch_number}: Wrote {len(batch)} rows to {out_file.name}")
            conn.commit()  # Also commit here
            batch = []
            batch_number += 1
            total_written += len(df)
        
        # Progress logging every 10000 rows
        if (idx + 1) % 10000 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = total_processed / elapsed if elapsed > 0 else 0
            LOGGER.info(f"Progress: Processed {total_processed:,} rows | Written {total_written:,} | "
                       f"Rate: {rate:.1f} rows/sec | Elapsed: {elapsed:.1f}s")

    # Write remaining batch
    if batch:
        df = pd.DataFrame(batch)
        out_file = out_dir / f"train_{batch_number:04d}.parquet"
        df.to_parquet(out_file, engine="pyarrow", index=False)
        LOGGER.info(f"✓ Final batch {batch_number}: Wrote {len(batch)} rows to {out_file.name}")
        conn.commit()
        total_written += len(df)

    # Final commit for any remaining hashes
    conn.commit()
    
    elapsed_total = (datetime.now() - start_time).total_seconds()
    
    LOGGER.info(f"="*80)
    LOGGER.info(f"INGESTION COMPLETE: {name}")
    LOGGER.info(f"Total processed: {total_processed:,}")
    LOGGER.info(f"Total written: {total_written:,}")
    LOGGER.info(f"Skipped (no column): {skipped_no_column:,}")
    LOGGER.info(f"Skipped (empty): {skipped_empty:,}")
    LOGGER.info(f"Skipped (duplicate): {skipped_duplicate:,}")
    LOGGER.info(f"Hashes inserted: {hash_insert_count:,}")
    LOGGER.info(f"Total time: {elapsed_total:.1f}s")
    LOGGER.info(f"Average rate: {total_processed/elapsed_total:.1f} rows/sec")
    LOGGER.info(f"="*80)

# --------------------------
# Iterate over datasets in sources.yaml
# --------------------------
LOGGER.info(f"Starting batch processing of {len(datasets_to_load)} datasets")

for idx, ds_info in enumerate(datasets_to_load, 1):
    try:
        LOGGER.info(f"\n[{idx}/{len(datasets_to_load)}] Processing dataset: {ds_info['name']}")
        ingest_dataset(ds_info)
    except Exception as e:
        LOGGER.exception(f"Failed to ingest {ds_info['name']}: {e}")
        LOGGER.error(f"Continuing to next dataset...")

conn.close()
LOGGER.info("\n" + "="*80)
LOGGER.info("ALL DATASETS PROCESSED")
LOGGER.info(f"Database location: {DB_PATH}")
LOGGER.info("="*80)