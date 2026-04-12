# scripts/preprocessing/segment_text.py
"""
Text segmentation for Nepali corpus preprocessing.

This script segments text into sentences and paragraphs for better NLP processing.
Uses Nepali-specific punctuation and linguistic rules.

Usage:
    python -m scripts.preprocessing.segment_text
"""

import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
import pyarrow.parquet as pq

from scripts.utils.paths import PROCESSED_DIR, METADATA_DIR, LOGS_PREPROCESSING
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# Logger
LOGGER = get_logger("preprocessing.segment_text", log_type="preprocessing")

# Load preprocessing config
PREPROCESSING_CONFIG = load_config("preprocessing.yaml")

# Nepali sentence segmentation patterns
NEPALI_SENTENCE_ENDINGS = r'[।॥!?…]'

# Sentence boundary patterns (Nepali punctuation)
SENTENCE_SPLIT_RE = re.compile(r'([।॥!?…])\s*')

# Paragraph boundary patterns
PARAGRAPH_SPLIT_RE = re.compile(r'\n\s*\n')

# Minimum sentence length (in words)
MIN_SENTENCE_WORDS = 3

# Maximum sentence length (in words) - to avoid very long sentences
MAX_SENTENCE_WORDS = 100

def segment_sentences(text: str, config: Dict) -> List[str]:
    """
    Segment text into sentences using Nepali punctuation.

    Args:
        text: Text to segment
        config: Preprocessing configuration

    Returns:
        List of sentences
    """
    if not isinstance(text, str) or not text.strip():
        return []

    # Split on Nepali sentence endings
    sentences = SENTENCE_SPLIT_RE.split(text)

    # Reconstruct sentences with punctuation
    reconstructed_sentences = []
    current_sentence = ""

    for part in sentences:
        current_sentence += part
        if SENTENCE_SPLIT_RE.match(part.strip()):
            # This is a sentence ending, save the current sentence
            sentence = current_sentence.strip()
            if sentence:
                reconstructed_sentences.append(sentence)
            current_sentence = ""
        elif part.strip() and not SENTENCE_SPLIT_RE.match(part.strip()):
            # Continue building sentence
            pass

    # Add any remaining text as a sentence
    if current_sentence.strip():
        reconstructed_sentences.append(current_sentence.strip())

    # Filter sentences by length
    min_words = config.get("min_sentence_length", MIN_SENTENCE_WORDS)
    max_words = config.get("max_sentence_length", MAX_SENTENCE_WORDS)

    filtered_sentences = []
    for sentence in reconstructed_sentences:
        words = sentence.split()
        if min_words <= len(words) <= max_words:
            filtered_sentences.append(sentence)

    return filtered_sentences

def segment_paragraphs(text: str, config: Dict) -> List[str]:
    """
    Segment text into paragraphs.

    Args:
        text: Text to segment
        config: Preprocessing configuration

    Returns:
        List of paragraphs
    """
    if not isinstance(text, str) or not text.strip():
        return []

    # Split on paragraph boundaries (double newlines)
    paragraphs = PARAGRAPH_SPLIT_RE.split(text)

    # Filter out empty paragraphs
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    # Minimum paragraph length
    min_length = config.get("min_paragraph_length", 20)
    paragraphs = [p for p in paragraphs if len(p) >= min_length]

    return paragraphs

def process_text(text: str, config: Dict) -> Dict[str, Any]:
    """
    Process text for both sentence and paragraph segmentation.

    Args:
        text: Text to process
        config: Preprocessing configuration

    Returns:
        Dictionary with segmented text
    """
    result = {
        "original_text": text,
        "sentences": [],
        "paragraphs": [],
        "sentence_count": 0,
        "paragraph_count": 0,
        "avg_sentence_length": 0,
        "avg_paragraph_length": 0
    }

    if not isinstance(text, str) or not text.strip():
        return result

    # Segment sentences
    if config.get("sentence_split", True):
        sentences = segment_sentences(text, config)
        result["sentences"] = sentences
        result["sentence_count"] = len(sentences)

        if sentences:
            total_words = sum(len(s.split()) for s in sentences)
            result["avg_sentence_length"] = total_words / len(sentences)

    # Segment paragraphs
    if config.get("paragraph_split", True):
        paragraphs = segment_paragraphs(text, config)
        result["paragraphs"] = paragraphs
        result["paragraph_count"] = len(paragraphs)

        if paragraphs:
            total_chars = sum(len(p) for p in paragraphs)
            result["avg_paragraph_length"] = total_chars / len(paragraphs)

    return result

def process_directory(input_dir: Path, output_dir: Path, config: Dict) -> Dict[str, Any]:
    """
    Process all parquet files in a directory for text segmentation.

    Args:
        input_dir: Directory containing input parquet files
        output_dir: Directory to write segmented parquet files
        config: Preprocessing configuration

    Returns:
        Statistics dictionary
    """
    LOGGER.info(f"Processing directory for text segmentation: {input_dir}")

    stats = {
        "input_files": 0,
        "output_files": 0,
        "input_rows": 0,
        "output_rows": 0,
        "errors": 0,
        "segmentation_stats": {
            "total_sentences": 0,
            "total_paragraphs": 0,
            "avg_sentences_per_doc": 0,
            "avg_paragraphs_per_doc": 0,
            "avg_sentence_length": 0,
            "avg_paragraph_length": 0
        }
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

            # Apply text segmentation
            segmentation_results = []
            for _, row in df.iterrows():
                segmentation = process_text(row["text"], config)
                segmentation_results.append(segmentation)

                # Accumulate stats
                stats["segmentation_stats"]["total_sentences"] += segmentation["sentence_count"]
                stats["segmentation_stats"]["total_paragraphs"] += segmentation["paragraph_count"]

            # Prepare output data
            output_data = []
            for (_, row), segmentation in zip(df.iterrows(), segmentation_results):
                output_row = {
                    "text": row["text"],  # Keep original text
                    "sentences": segmentation["sentences"],
                    "paragraphs": segmentation["paragraphs"],
                    "sentence_count": segmentation["sentence_count"],
                    "paragraph_count": segmentation["paragraph_count"],
                    "avg_sentence_length": segmentation["avg_sentence_length"],
                    "avg_paragraph_length": segmentation["avg_paragraph_length"],
                    "source": row.get("source", "unknown"),
                    "url": row.get("url", ""),
                    "title": row.get("title", ""),
                    "date": row.get("date", ""),
                    "dataset_name": input_dir.name,
                    "language_confidence": row.get("language_confidence", 0.0),
                    "devanagari_ratio": row.get("devanagari_ratio", 0.0)
                }
                output_data.append(output_row)

            # Create output dataframe
            output_df = pd.DataFrame(output_data)
            stats["output_rows"] += len(output_df)

            # Calculate averages
            if stats["output_rows"] > 0:
                stats["segmentation_stats"]["avg_sentences_per_doc"] = stats["segmentation_stats"]["total_sentences"] / stats["output_rows"]
                stats["segmentation_stats"]["avg_paragraphs_per_doc"] = stats["segmentation_stats"]["total_paragraphs"] / stats["output_rows"]

                # Calculate weighted averages for lengths
                total_sentence_words = sum(row["avg_sentence_length"] * row["sentence_count"] for row in output_data if row["sentence_count"] > 0)
                total_sentences = sum(row["sentence_count"] for row in output_data)
                if total_sentences > 0:
                    stats["segmentation_stats"]["avg_sentence_length"] = total_sentence_words / total_sentences

                total_paragraph_chars = sum(row["avg_paragraph_length"] * row["paragraph_count"] for row in output_data if row["paragraph_count"] > 0)
                total_paragraphs = sum(row["paragraph_count"] for row in output_data)
                if total_paragraphs > 0:
                    stats["segmentation_stats"]["avg_paragraph_length"] = total_paragraph_chars / total_paragraphs

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
    """Main text segmentation function."""
    LOGGER.info("="*80)
    LOGGER.info("✂️  TEXT SEGMENTATION PREPROCESSING")
    LOGGER.info("Segmenting text into sentences and paragraphs")
    LOGGER.info("="*80)

    start_time = datetime.now()

    # Input and output directories
    input_base = PROCESSED_DIR / "language_filtered"
    output_base = PROCESSED_DIR / "segmented"

    if not input_base.exists():
        LOGGER.error(f"Input directory not found: {input_base}")
        LOGGER.error("Run language filtering first!")
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
        "errors": 0,
        "segmentation_stats": {
            "total_sentences": 0,
            "total_paragraphs": 0,
            "avg_sentences_per_doc": 0,
            "avg_paragraphs_per_doc": 0,
            "avg_sentence_length": 0,
            "avg_paragraph_length": 0
        }
    }

    # Process each site
    for site_dir in site_dirs:
        site_name = site_dir.name
        LOGGER.info(f"Processing site: {site_name}")

        output_dir = output_base / site_name
        site_stats = process_directory(site_dir, output_dir, PREPROCESSING_CONFIG)

        # Accumulate stats
        total_stats["sites_processed"] += 1
        for key in ["input_files", "output_files", "input_rows", "output_rows", "errors"]:
            total_stats[key] += site_stats[key]

        # Accumulate segmentation stats
        for seg_key in total_stats["segmentation_stats"]:
            total_stats["segmentation_stats"][seg_key] += site_stats["segmentation_stats"][seg_key]

    # Save statistics
    stats_file = METADATA_DIR / "preprocessing_stats" / f"segment_text_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
    LOGGER.info("✂️  TEXT SEGMENTATION COMPLETED")
    LOGGER.info(f"⏱️  Duration: {duration}")
    LOGGER.info(f"📁 Sites processed: {total_stats['sites_processed']}")
    LOGGER.info(f"📄 Files: {total_stats['input_files']} → {total_stats['output_files']}")
    LOGGER.info(f"📊 Rows: {total_stats['input_rows']:,} → {total_stats['output_rows']:,}")
    LOGGER.info(f"❌ Errors: {total_stats['errors']}")
    LOGGER.info("✂️  Segmentation Statistics:")
    LOGGER.info(f"  📝 Total sentences: {total_stats['segmentation_stats']['total_sentences']:,}")
    LOGGER.info(f"  📄 Total paragraphs: {total_stats['segmentation_stats']['total_paragraphs']:,}")
    LOGGER.info(f"  📊 Avg sentences per document: {total_stats['segmentation_stats']['avg_sentences_per_doc']:.1f}")
    LOGGER.info(f"  📊 Avg paragraphs per document: {total_stats['segmentation_stats']['avg_paragraphs_per_doc']:.1f}")
    LOGGER.info(f"  📏 Avg sentence length: {total_stats['segmentation_stats']['avg_sentence_length']:.1f} words")
    LOGGER.info(f"  📏 Avg paragraph length: {total_stats['segmentation_stats']['avg_paragraph_length']:.1f} chars")
    LOGGER.info(f"📈 Stats saved to: {stats_file}")
    LOGGER.info("="*80)

    return 0

if __name__ == "__main__":
    exit(main())