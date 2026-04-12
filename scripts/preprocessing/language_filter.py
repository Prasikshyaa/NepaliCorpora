# scripts/preprocessing/language_filter.py
"""
Language filtering for Nepali corpus preprocessing.

This script filters text to ensure it contains primarily Nepali language content.
Uses statistical language detection and Devanagari script validation.

Usage:
    python -m scripts.preprocessing.language_filter
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
LOGGER = get_logger("preprocessing.language_filter", log_type="preprocessing")

# Load preprocessing config
PREPROCESSING_CONFIG = load_config("preprocessing.yaml")

# Language detection patterns
DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]+")
NEPALI_COMMON_WORDS = {
    'छ', 'ले', 'को', 'मा', 'बाट', 'सँग', 'लाई', 'का', 'की', 'का', 'हरु', 'हरू',
    'गरे', 'भए', 'हुने', 'गर्ने', 'भएको', 'गरेको', 'हुनेछ', 'गर्नेछ',
    'नेपाल', 'काठमाडौँ', 'पोखरा', 'ललितपुर', 'भक्तपुर', 'बिराटनगर', 'धरान',
    'जनकपुर', 'विरगञ्ज', 'सप्तरी', 'सिराहा', 'महोत्तरी', 'सर्लाही', 'रौतहट',
    'बारा', 'पर्सा', 'चितवन', 'मकवानपुर', 'धादिङ', 'नुवाकोट', 'रसुवा',
    'गोरखा', 'लमजुङ', 'तनहुँ', 'स्याङ्जा', 'कास्की', 'मनाङ', 'मुस्ताङ',
    'म्याग्दी', 'पर्वत', 'बाग्लुङ', 'गुल्मी', 'पाल्पा', 'नवलपरासी', 'रुपन्देही',
    'कपिलवस्तु', 'अर्घाखाँची', 'प्यूठान', 'रोल्पा', 'रुकुम', 'सल्यान',
    'दाङ', 'बाँके', 'बर्दिया', 'सुर्खेत', 'दैलेख', 'जाजरकोट', 'डोल्पा',
    'जुम्ला', 'कालिकोट', 'मुगु', 'हुम्ला', 'बाजुरा', 'बझाङ', 'अछाम',
    'डोटी', 'कैलाली', 'कञ्चनपुर', 'डडेल्धुरा', 'बैतडी', 'दार्चुला'
}

def detect_nepali_content(text: str, config: Dict) -> Dict[str, Any]:
    """
    Detect if text contains primarily Nepali language content.

    Args:
        text: Text to analyze
        config: Preprocessing configuration

    Returns:
        Dictionary with detection results and confidence score
    """
    if not isinstance(text, str) or not text.strip():
        return {"is_nepali": False, "confidence": 0.0, "reason": "empty_text"}

    # Check for Devanagari script presence
    devanagari_matches = DEVANAGARI_RE.findall(text)
    devanagari_chars = sum(len(match) for match in devanagari_matches)
    total_chars = len(text)

    if total_chars == 0:
        return {"is_nepali": False, "confidence": 0.0, "reason": "no_characters"}

    devanagari_ratio = devanagari_chars / total_chars

    # Minimum Devanagari ratio required
    min_devanagari_ratio = config.get("min_devanagari_ratio", 0.3)

    if devanagari_ratio < min_devanagari_ratio:
        return {
            "is_nepali": False,
            "confidence": devanagari_ratio,
            "reason": f"low_devanagari_ratio_{devanagari_ratio:.2f}"
        }

    # Check for Nepali common words using Devanagari word tokens
    words = re.findall(r'[\u0900-\u097F]{2,}', text)
    nepali_word_count = sum(1 for word in words if word in NEPALI_COMMON_WORDS)
    total_words = len(words)

    if total_words > 0:
        nepali_word_ratio = nepali_word_count / total_words
        # Boost confidence if Nepali words are found
        confidence = min(1.0, devanagari_ratio + (nepali_word_ratio * 0.3))
    else:
        confidence = devanagari_ratio

    # Minimum confidence threshold
    min_confidence = config.get("min_language_confidence", 0.5)

    is_nepali = confidence >= min_confidence

    return {
        "is_nepali": is_nepali,
        "confidence": confidence,
        "reason": f"confidence_{confidence:.2f}",
        "devanagari_ratio": devanagari_ratio,
        "nepali_words": nepali_word_count,
        "total_words": total_words
    }

def filter_language(text: str, config: Dict) -> str:
    """
    Filter text based on language detection.

    Args:
        text: Text to filter
        config: Preprocessing configuration

    Returns:
        Filtered text or None if it should be removed
    """
    detection = detect_nepali_content(text, config)

    if detection["is_nepali"]:
        return text
    else:
        return None

def process_directory(input_dir: Path, output_dir: Path, config: Dict) -> Dict[str, Any]:
    """
    Process all parquet files in a directory for language filtering.

    Args:
        input_dir: Directory containing input parquet files
        output_dir: Directory to write filtered parquet files
        config: Preprocessing configuration

    Returns:
        Statistics dictionary
    """
    LOGGER.info(f"Processing directory for language filtering: {input_dir}")

    stats = {
        "input_files": 0,
        "output_files": 0,
        "input_rows": 0,
        "output_rows": 0,
        "filtered_rows": 0,
        "errors": 0,
        "language_stats": {
            "high_confidence": 0,
            "medium_confidence": 0,
            "low_confidence": 0,
            "rejected": 0
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

            # Apply language filtering
            df["language_detection"] = df["text"].apply(lambda x: detect_nepali_content(x, config))
            df["filtered_text"] = df["text"].apply(lambda x: filter_language(x, config))

            # Update language statistics
            for detection in df["language_detection"]:
                confidence = detection["confidence"]
                if detection["is_nepali"]:
                    if confidence >= 0.8:
                        stats["language_stats"]["high_confidence"] += 1
                    elif confidence >= 0.6:
                        stats["language_stats"]["medium_confidence"] += 1
                    else:
                        stats["language_stats"]["low_confidence"] += 1
                else:
                    stats["language_stats"]["rejected"] += 1

            # Filter out None values
            df_clean = df[df["filtered_text"].notna()].copy()
            filtered_rows = input_rows - len(df_clean)
            stats["filtered_rows"] += filtered_rows

            if len(df_clean) == 0:
                LOGGER.warning(f"All rows filtered out from {parquet_file}")
                continue

            # Prepare output data
            output_data = []
            for _, row in df_clean.iterrows():
                output_row = {
                    "text": row["filtered_text"],
                    "source": row.get("source", "unknown"),
                    "url": row.get("url", ""),
                    "title": row.get("title", ""),
                    "date": row.get("date", ""),
                    "dataset_name": input_dir.name,
                    "language_confidence": row["language_detection"]["confidence"],
                    "devanagari_ratio": row["language_detection"]["devanagari_ratio"]
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
    """Main language filtering function."""
    LOGGER.info("="*80)
    LOGGER.info("🌍 LANGUAGE FILTERING PREPROCESSING")
    LOGGER.info("Filtering content to ensure Nepali language dominance")
    LOGGER.info("="*80)

    start_time = datetime.now()

    # Input and output directories
    input_base = PROCESSED_DIR / "preprocessed"
    output_base = PROCESSED_DIR / "language_filtered"

    if not input_base.exists():
        LOGGER.error(f"Input directory not found: {input_base}")
        LOGGER.error("Run text cleaning first!")
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
        "errors": 0,
        "language_stats": {
            "high_confidence": 0,
            "medium_confidence": 0,
            "low_confidence": 0,
            "rejected": 0
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
        for key in ["input_files", "output_files", "input_rows", "output_rows", "filtered_rows", "errors"]:
            total_stats[key] += site_stats[key]

        # Accumulate language stats
        for lang_key in total_stats["language_stats"]:
            total_stats["language_stats"][lang_key] += site_stats["language_stats"][lang_key]

    # Save statistics
    stats_file = METADATA_DIR / "preprocessing_stats" / f"language_filter_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
    LOGGER.info("🌍 LANGUAGE FILTERING COMPLETED")
    LOGGER.info(f"⏱️  Duration: {duration}")
    LOGGER.info(f"📁 Sites processed: {total_stats['sites_processed']}")
    LOGGER.info(f"📄 Files: {total_stats['input_files']} → {total_stats['output_files']}")
    LOGGER.info(f"📊 Rows: {total_stats['input_rows']:,} → {total_stats['output_rows']:,}")
    LOGGER.info(f"🗑️  Filtered: {total_stats['filtered_rows']:,} rows")
    LOGGER.info(f"❌ Errors: {total_stats['errors']}")
    LOGGER.info("🌍 Language Distribution:")
    LOGGER.info(f"  🟢 High confidence (≥80%): {total_stats['language_stats']['high_confidence']:,}")
    LOGGER.info(f"  🟡 Medium confidence (60-80%): {total_stats['language_stats']['medium_confidence']:,}")
    LOGGER.info(f"  🟠 Low confidence (50-60%): {total_stats['language_stats']['low_confidence']:,}")
    LOGGER.info(f"  🔴 Rejected (<50%): {total_stats['language_stats']['rejected']:,}")
    LOGGER.info(f"📈 Stats saved to: {stats_file}")
    LOGGER.info("="*80)

    return 0

if __name__ == "__main__":
    exit(main())