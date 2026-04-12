# scripts/preprocessing/preprocess_sakoni_resumable.py
"""
Resumable preprocessing script for Sakonii_nepalitext-language-model-dataset
- Cleans raw parquet files from RAW_READY_MADE/Sakonii_nepalitext-language-model-dataset
- Applies rules from preprocessing.yaml
- Writes cleaned batches to PROCESSED_DIR/huggingface
- Saves preprocessing stats to METADATA_DIR/preprocessing_stats
- Tracks progress for resumability
"""

import re
import unicodedata
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import pandas as pd
import pyarrow.parquet as pq

from scripts.utils.paths import RAW_READY_MADE, PROCESSED_DIR, METADATA_DIR
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# --------------------------
# Logger
# --------------------------
LOGGER = get_logger("preprocess_sakoni_resumable", log_type="preprocessing")

# --------------------------
# Load preprocessing config
# --------------------------
PREPROCESSING_CONFIG = load_config("preprocessing.yaml")

# --------------------------
# Regex patterns
# --------------------------
DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]+")
URL_RE = re.compile(r"https?://\S+|www\.\S+")
EMAIL_RE = re.compile(r"\S+@\S+")
HTML_TAG_RE = re.compile(r'<[^>]+>')
EXCESSIVE_WHITESPACE_RE = re.compile(r'\s+')
EXCESSIVE_NEWLINES_RE = re.compile(r'\n{3,}')
REPEATED_PUNCTUATION_RE = re.compile(r'([।॥!?.,:;])\1{2,}')
CONTROL_CHARS_RE = re.compile(r'[\x00-\x1F\x7F-\x9F]')

# --------------------------
# Vectorized cleaning
# --------------------------
def clean_text_vectorized(series: pd.Series, config: Dict) -> pd.Series:
    """Vectorized Nepali text cleaning based on config rules."""
    result = series.copy()
    result = result.where(result.notna() & (result.str.len() > 0), None)

    if config.get("remove_html", True):
        result = result.str.replace(HTML_TAG_RE, "", regex=True)
    if config.get("remove_control_chars", False):
        result = result.str.replace(CONTROL_CHARS_RE, "", regex=True)
    if config.get("remove_urls", True):
        result = result.str.replace(URL_RE, "", regex=True)
    if config.get("remove_emails", True):
        result = result.str.replace(EMAIL_RE, "", regex=True)
    if config.get("normalize_unicode", True):
        result = result.apply(lambda x: unicodedata.normalize("NFC", x) if isinstance(x, str) else x)
    if config.get("normalize_newlines", False):
        result = result.str.replace(EXCESSIVE_NEWLINES_RE, "\n\n", regex=True)
    if config.get("normalize_whitespace", True):
        result = result.str.replace(EXCESSIVE_WHITESPACE_RE, " ", regex=True)
    if config.get("normalize_punctuation", False):
        result = result.str.replace(REPEATED_PUNCTUATION_RE, r'\1', regex=True)
    if config.get("strip_whitespace", True):
        result = result.str.strip()
    if config.get("remove_non_nepali", True):
        has_devanagari = result.str.contains(DEVANAGARI_RE, na=False)
        result = result.where(has_devanagari, None)
    min_len = config.get("min_document_length", 20)
    result = result.where(result.str.len() >= min_len, None)
    max_len = config.get("max_document_length", 0)
    if max_len > 0:
        result = result.where(result.str.len() <= max_len, None)
    return result

# --------------------------
# Write batch safely
# --------------------------
def write_batch(df: pd.DataFrame, output_dir: Path, batch_idx: int) -> bool:
    try:
        out_file = output_dir / f"cleaned_{batch_idx:04d}.parquet"
        df.to_parquet(out_file, index=False, engine='pyarrow')
        return True
    except Exception as e:
        LOGGER.error(f"Failed to write batch {batch_idx}: {e}")
        return False

# --------------------------
# Save statistics
# --------------------------
def save_stats(dataset_name: str, stats: Dict[str, Any]):
    stats_dir = METADATA_DIR / "preprocessing_stats"
    stats_dir.mkdir(parents=True, exist_ok=True)
    stats_file = stats_dir / f"{dataset_name}_stats.json"
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    LOGGER.info(f"Statistics saved to {stats_file}")

# --------------------------
# Save checkpoint for resumability
# --------------------------
def save_checkpoint(output_dir: Path, batch_idx: int):
    checkpoint_file = output_dir / "last_batch.json"
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump({"last_batch_idx": batch_idx}, f)
        
def load_checkpoint(output_dir: Path) -> int:
    checkpoint_file = output_dir / "last_batch.json"
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get("last_batch_idx", 0)
    return 0

# --------------------------
# Main preprocessing
# --------------------------
def preprocess_sakoni_resumable():
    dataset_name = "Sakonii_nepalitext-language-model-dataset"
    input_dir = RAW_READY_MADE / dataset_name
    output_dir = PROCESSED_DIR / "huggingface" / dataset_name
    output_dir.mkdir(parents=True, exist_ok=True)
    # Track which raw files have already been processed (file-level resumability)
    processed_marker_dir = output_dir / "_processed_files"
    processed_marker_dir.mkdir(parents=True, exist_ok=True)


    parquet_files = sorted(input_dir.glob("*.parquet"))
    if not parquet_files:
        LOGGER.warning(f"No parquet files found in {input_dir}")
        return

    batch_size = PREPROCESSING_CONFIG.get("output_batch_size", 10000)
    accumulated_rows = []

    # Load last batch index for resumability
    batch_idx = load_checkpoint(output_dir)
    LOGGER.info(f"Resuming from batch index: {batch_idx+1}")

    # Stats
    total_input_rows = 0
    total_output_rows = 0
    total_filtered = 0
    files_processed = 0
    files_failed = 0
    batches_written = batch_idx
    batches_failed = 0

    start_time = datetime.now()

    for file_num, pf in enumerate(parquet_files, 1):
        # Skip already processed files
        marker = processed_marker_dir / f"{pf.stem}.done"
        if marker.exists():
            LOGGER.info(f"Skipping already processed file: {pf.name}")
            continue
        LOGGER.info(f"[{file_num}/{len(parquet_files)}] {pf.name}")
        LOGGER.info(f"Processing raw file: {pf.name}")

        try:
            df = pq.read_table(pf).to_pandas()
            if "text" not in df.columns:
                LOGGER.error(f"'text' column missing in {pf.name}")
                files_failed += 1
                continue

            input_rows = len(df)
            total_input_rows += input_rows

            df['cleaned_text'] = clean_text_vectorized(df['text'], PREPROCESSING_CONFIG)
            df_clean = df[df['cleaned_text'].notna()].copy()

            filtered = input_rows - len(df_clean)
            total_filtered += filtered

            if len(df_clean) == 0:
                LOGGER.warning(f"No valid rows after cleaning (filtered all {filtered:,} rows)")
                marker.touch() 
                files_processed += 1
                continue

            available_columns = df.columns.tolist()
            output_rows = []
            for _, row in df_clean.iterrows():
                output_rows.append({
                    "text": row['cleaned_text'],
                    "source": row.get('source') if 'source' in available_columns else row.get('Source'),
                    "dataset_name": row.get('dataset_name', dataset_name)
                })

            accumulated_rows.extend(output_rows)

            while len(accumulated_rows) >= batch_size:
                batch_df = pd.DataFrame(accumulated_rows[:batch_size])
                batch_idx += 1
                if write_batch(batch_df, output_dir, batch_idx):
                    batches_written += 1
                    total_output_rows += len(batch_df)
                    save_checkpoint(output_dir, batch_idx)
                else:
                    batches_failed += 1
                accumulated_rows = accumulated_rows[batch_size:]
            marker.touch()
            files_processed += 1

        except Exception as e:
            LOGGER.exception(f"Error processing {pf.name}: {e}")
            files_failed += 1
            continue

    # Write remaining rows
    if accumulated_rows:
        batch_df = pd.DataFrame(accumulated_rows)
        batch_idx += 1
        if write_batch(batch_df, output_dir, batch_idx):
            batches_written += 1
            total_output_rows += len(batch_df)
            save_checkpoint(output_dir, batch_idx)
        else:
            batches_failed += 1

    # Save stats
    elapsed = (datetime.now() - start_time).total_seconds()
    filter_rate = (total_filtered / total_input_rows * 100) if total_input_rows > 0 else 0
    stats = {
        "dataset_name": dataset_name,
        "timestamp": datetime.now().isoformat(),
        "input_rows": total_input_rows,
        "output_rows": total_output_rows,
        "filtered_rows": total_filtered,
        "filter_percentage": round(filter_rate, 2),
        "files_processed": files_processed,
        "files_failed": files_failed,
        "batches_written": batches_written,
        "batches_failed": batches_failed,
        "time_seconds": round(elapsed, 2),
        "rows_per_second": round(total_input_rows / elapsed, 2) if elapsed > 0 else 0
    }
    save_stats(dataset_name, stats)

    LOGGER.info(f"Preprocessing complete: {total_output_rows:,} rows written, {total_filtered:,} rows filtered.")

# --------------------------
# CLI entry point
# --------------------------
if __name__ == "__main__":
    preprocess_sakoni_resumable()
