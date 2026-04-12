# scripts/preprocessing/clean_text.py
"""
General text cleaning script for Nepali corpus preprocessing.

This script processes scraped news articles from data/processed/huggingface/
and outputs cleaned text to data/processed/preprocessed/

Handles multiple news sites and applies comprehensive text cleaning:
- Unicode normalization
- HTML tag removal
- URL and email removal
- Excessive whitespace cleanup
- Control character removal
- Devanagari script validation

Usage:
    python -m scripts.preprocessing.clean_text
"""

import re
import unicodedata
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
import pyarrow.parquet as pq

from scripts.utils.paths import PROCESSED_DIR, METADATA_DIR, LOGS_PREPROCESSING
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# Logger
LOGGER = get_logger("preprocessing.clean_text", log_type="preprocessing")

# Load preprocessing config
PREPROCESSING_CONFIG = load_config("preprocessing.yaml")

# Regex patterns for text cleaning
DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]+")
URL_RE = re.compile(r"https?://\S+|www\.\S+")
EMAIL_RE = re.compile(r"\S+@\S+")
HTML_TAG_RE = re.compile(r'<[^>]+>')
EXCESSIVE_WHITESPACE_RE = re.compile(r'\s+')
EXCESSIVE_NEWLINES_RE = re.compile(r'\n{3,}')
REPEATED_PUNCTUATION_RE = re.compile(r'([।॥!?.,:;])\1{2,}')
CONTROL_CHARS_RE = re.compile(r'[\x00-\x1F\x7F-\x9F]')

def clean_text(text: str, config: Dict) -> str:
    """
    Clean a single text string based on configuration rules.

    Args:
        text: Raw text to clean
        config: Preprocessing configuration

    Returns:
        Cleaned text or None if text should be filtered out
    """
    if not isinstance(text, str) or not text.strip():
        return None

    # Unicode normalization
    if config.get("unicode_normalize", True):
        text = unicodedata.normalize('NFC', text)

    # Remove HTML tags
    if config.get("remove_html", True):
        text = HTML_TAG_RE.sub('', text)

    # Remove URLs
    if config.get("remove_urls", True):
        text = URL_RE.sub('', text)

    # Remove emails
    if config.get("remove_emails", True):
        text = EMAIL_RE.sub('', text)

    # Remove control characters
    if config.get("remove_control_chars", True):
        text = CONTROL_CHARS_RE.sub('', text)

    # Clean excessive whitespace
    if config.get("clean_whitespace", True):
        text = EXCESSIVE_WHITESPACE_RE.sub(' ', text)
        text = EXCESSIVE_NEWLINES_RE.sub('\n\n', text)
        text = text.strip()

    # Remove repeated punctuation
    if config.get("remove_repeated_punct", True):
        text = REPEATED_PUNCTUATION_RE.sub(r'\1\1', text)

    # Minimum length filter
    min_length = config.get("min_text_length", 10)
    if len(text) < min_length:
        return None

    # Devanagari script validation
    if config.get("require_devanagari", True):
        if not DEVANAGARI_RE.search(text):
            return None

    return text

def process_directory(input_dir: Path, output_dir: Path, config: Dict) -> Dict[str, Any]:
    """
    Process all parquet files in a directory.

    Args:
        input_dir: Directory containing input parquet files
        output_dir: Directory to write cleaned parquet files
        config: Preprocessing configuration

    Returns:
        Statistics dictionary
    """
    LOGGER.info(f"Processing directory: {input_dir}")

    stats = {
        "input_files": 0,
        "output_files": 0,
        "input_rows": 0,
        "output_rows": 0,
        "filtered_rows": 0,
        "errors": 0
    }

    # Find all parquet files
    parquet_files = list(input_dir.rglob("*.parquet"))
    if not parquet_files:
        LOGGER.warning(f"No parquet files found in {input_dir}")
        return stats

    stats["input_files"] = len(parquet_files)
    LOGGER.info(f"Found {len(parquet_files)} parquet files")

    for parquet_file in parquet_files:
        try:
            # Read parquet file
            table = pq.read_table(parquet_file)
            df = table.to_pandas()

            if "text" not in df.columns:
                LOGGER.warning(f"No 'text' column in {parquet_file}, skipping")
                stats["errors"] += 1
                continue

            input_rows = len(df)
            stats["input_rows"] += input_rows

            # Apply cleaning
            df["cleaned_text"] = df["text"].apply(lambda x: clean_text(x, config))

            # Filter out None values
            df_clean = df[df["cleaned_text"].notna()].copy()
            filtered_rows = input_rows - len(df_clean)
            stats["filtered_rows"] += filtered_rows

            if len(df_clean) == 0:
                LOGGER.warning(f"All rows filtered out from {parquet_file}")
                continue

            # Prepare output data
            output_data = []
            for _, row in df_clean.iterrows():
                output_row = {
                    "text": row["cleaned_text"],
                    "source": row.get("source", "unknown"),
                    "url": row.get("url", ""),
                    "title": row.get("title", ""),
                    "date": row.get("date", ""),
                    "dataset_name": input_dir.name
                }
                output_data.append(output_row)

            # Create output dataframe
            output_df = pd.DataFrame(output_data)
            stats["output_rows"] += len(output_df)

            # Determine output path
            relative_path = parquet_file.relative_to(input_dir)
            output_file = output_dir / relative_path
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Write to parquet
            output_df.to_parquet(output_file, index=False)
            stats["output_files"] += 1

            LOGGER.info(f"Processed {parquet_file.name}: {input_rows} → {len(output_df)} rows")

        except Exception as e:
            LOGGER.error(f"Error processing {parquet_file}: {e}")
            stats["errors"] += 1

    return stats

def main():
    """Main preprocessing function."""
    LOGGER.info("="*80)
    LOGGER.info("🧹 TEXT CLEANING PREPROCESSING")
    LOGGER.info("Cleaning scraped news articles for Nepali corpus")
    LOGGER.info("="*80)

    start_time = datetime.now()

    # Input and output directories
    input_base = PROCESSED_DIR / "huggingface"
    output_base = PROCESSED_DIR / "preprocessed"

    if not input_base.exists():
        LOGGER.error(f"Input directory not found: {input_base}")
        return 1

    # Get all subdirectories (news sites)
    site_dirs = [d for d in input_base.iterdir() if d.is_dir()]
    if not site_dirs:
        LOGGER.warning(f"No site directories found in {input_base}")
        return 1

    LOGGER.info(f"Found {len(site_dirs)} site directories to process")

    total_stats = {
        "sites_processed": 0,
        "input_files": 0,
        "output_files": 0,
        "input_rows": 0,
        "output_rows": 0,
        "filtered_rows": 0,
        "errors": 0
    }

    # Process each site
    for site_dir in site_dirs:
        site_name = site_dir.name
        LOGGER.info(f"Processing site: {site_name}")

        output_dir = output_base / site_name
        site_stats = process_directory(site_dir, output_dir, PREPROCESSING_CONFIG)

        # Accumulate stats
        total_stats["sites_processed"] += 1
        for key in ["input_files", "output_files", "input_rows", "output_rows", "filtered_rows", "errors"]:
            total_stats[key] += site_stats[key]

    # Save statistics
    stats_file = METADATA_DIR / "preprocessing_stats" / f"clean_text_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)

    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
            "config": PREPROCESSING_CONFIG,
            **total_stats
        }, f, indent=2, ensure_ascii=False)

    # Summary
    duration = datetime.now() - start_time
    LOGGER.info("="*80)
    LOGGER.info("🎉 TEXT CLEANING COMPLETED")
    LOGGER.info(f"⏱️  Duration: {duration}")
    LOGGER.info(f"📁 Sites processed: {total_stats['sites_processed']}")
    LOGGER.info(f"📄 Files: {total_stats['input_files']} → {total_stats['output_files']}")
    LOGGER.info(f"📊 Rows: {total_stats['input_rows']:,} → {total_stats['output_rows']:,}")
    LOGGER.info(f"🗑️  Filtered: {total_stats['filtered_rows']:,} rows")
    LOGGER.info(f"❌ Errors: {total_stats['errors']}")
    LOGGER.info(f"📈 Stats saved to: {stats_file}")
    LOGGER.info("="*80)

    return 0

if __name__ == "__main__":
    exit(main())
