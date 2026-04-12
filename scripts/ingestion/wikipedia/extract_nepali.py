"""
Full production-grade Nepali Wikipedia article extractor.
Implements all wikipedia.yaml configuration options with:
- Complete cleaning pipeline (templates, infoboxes, tables, refs, etc.)
- Paragraph-level filtering
- Devanagari ratio validation
- Metadata field selection
- Comprehensive skip reason tracking
- Memory-efficient streaming
- Restart-safe atomic writes
- **DUPLICATE PREVENTION: Always clears old files by default (resume=False)**

IMPORTANT: Wikipedia dumps are CUMULATIVE (contain all articles up to download date).
Running extraction multiple times on the same dump will create duplicates in your database.
Default behavior ALWAYS clears existing output files before extraction.
Only use --resume flag to continue an interrupted extraction run.

FIXED: XML namespace and binary mode for lxml compatibility
"""

import bz2
import json
import re
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import Iterator, Dict, Any, Optional, Set, List
from lxml import etree

from scripts.utils.paths import RAW_DIR, PROCESSED_DIR
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# ============================================================================
# LOGGER
# ============================================================================
LOGGER = get_logger("extract_wiki", log_type="wikipedia")

# ============================================================================
# XML NAMESPACE - FIXED TO 0.11
# ============================================================================
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.11/}"

# ============================================================================
# NEPALI PUNCTUATION NORMALIZATION MAP
# ============================================================================
NEPALI_PUNCTUATION_MAP = {
    '।।': '।',      # Double danda to single
    '॥॥': '॥',      # Double double-danda to single
    '...': '…',      # Triple dot to ellipsis
    '!!': '!',       # Double exclamation
    '??': '?',       # Double question
    ',,': ',',       # Double comma
}

# ============================================================================
# VALIDATION
# ============================================================================
def validate_dependencies():
    """Validate required dependencies."""
    try:
        import mwparserfromhell
    except ImportError:
        raise ImportError("mwparserfromhell not installed. Install: pip install mwparserfromhell")
    
    try:
        import pandas
    except ImportError:
        raise ImportError("pandas not installed. Install: pip install pandas")
    
    try:
        import pyarrow
    except ImportError:
        raise ImportError("pyarrow not installed. Install: pip install pyarrow")


def validate_config(config: Dict[str, Any]):
    """Validate required configuration fields."""
    required = [
        ("paths", "raw_dump_dir"),
        ("paths", "dump_filename"),
        ("paths", "processed_dir"),
        ("extraction", "allowed_namespaces"),
        ("cleaning", "require_devanagari"),
        ("output", "format"),
        ("output", "batch_size"),
    ]
    
    for *path, field in required:
        obj = config
        for key in path:
            if key not in obj:
                raise KeyError(f"Missing config section: {'.'.join(path)}")
            obj = obj[key]
        if field not in obj:
            raise KeyError(f"Missing config: {'.'.join(path)}.{field}")


# ============================================================================
# SKIP REASONS TRACKING
# ============================================================================
class SkipCounter:
    """Comprehensive tracking of article skip reasons."""
    
    def __init__(self):
        self.counts = {
            # Structural issues
            "wrong_namespace": 0,
            "no_text": 0,
            "no_revision": 0,
            
            # Content type filters
            "is_redirect": 0,
            "is_disambiguation": 0,
            
            # Length filters
            "too_short": 0,
            "too_long": 0,
            
            # Language filters
            "no_devanagari": 0,
            "low_devanagari_ratio": 0,
            
            # Cleaning failures
            "cleaning_failed": 0,
            "all_paragraphs_filtered": 0,
        }
    
    def increment(self, reason: str):
        """Increment counter for skip reason."""
        if reason in self.counts:
            self.counts[reason] += 1
        else:
            LOGGER.warning(f"Unknown skip reason: {reason}")
    
    def get_total(self) -> int:
        """Get total skipped count."""
        return sum(self.counts.values())
    
    def log_summary(self):
        """Log skip statistics."""
        total = self.get_total()
        if total == 0:
            LOGGER.info("No articles skipped")
            return
        
        LOGGER.info(f"Skip reasons (total: {total:,}):")
        for reason, count in sorted(self.counts.items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                pct = (count / total * 100) if total > 0 else 0
                LOGGER.info(f"  {reason}: {count:,} ({pct:.1f}%)")


# ============================================================================
# REDIRECT DETECTION
# ============================================================================
def is_redirect_raw(wikitext: str) -> bool:
    """
    Detect redirect pages from raw wikitext (before cleaning).
    
    Args:
        wikitext: Raw Wikipedia markup
        
    Returns:
        True if page is a redirect
    """
    if not wikitext:
        return False
    
    first_line = wikitext[:200].strip().lower()
    
    redirect_patterns = [
        r"^#redirect",
        r"^#पुनर्निर्देशन",
        r"^#अनुप्रेषित",
    ]
    
    return any(re.match(p, first_line, re.IGNORECASE) for p in redirect_patterns)


# ============================================================================
# DEVANAGARI RATIO CALCULATION
# ============================================================================
def calculate_devanagari_ratio(text: str) -> float:
    """
    Calculate percentage of Devanagari characters in text.
    
    Args:
        text: Text to analyze
        
    Returns:
        Ratio (0.0 to 1.0)
    """
    if not text:
        return 0.0
    
    # Count Devanagari characters
    devanagari_count = len(re.findall(r'[\u0900-\u097F]', text))
    
    # Count total meaningful characters (exclude spaces/punctuation)
    total_chars = len(re.sub(r'[\s\W\d]', '', text))
    
    if total_chars == 0:
        return 0.0
    
    return devanagari_count / total_chars


# ============================================================================
# PARAGRAPH PROCESSING
# ============================================================================
def filter_paragraphs(text: str, config: Dict) -> List[str]:
    """
    Split text into paragraphs and filter by length.
    
    Args:
        text: Cleaned text
        config: Cleaning configuration
        
    Returns:
        List of valid paragraphs
    """
    min_len = config.get("min_paragraph_length", 20)
    max_len = config.get("max_paragraph_length", 10000)
    
    # Split by multiple newlines
    paragraphs = re.split(r'\n\s*\n', text)
    
    valid_paragraphs = []
    for para in paragraphs:
        para = para.strip()
        if min_len <= len(para) <= max_len:
            valid_paragraphs.append(para)
    
    return valid_paragraphs


# ============================================================================
# COMPREHENSIVE TEXT CLEANING
# ============================================================================
def clean_wikitext(wikitext: str, config: Dict) -> Optional[str]:
    """
    Comprehensive Wikipedia markup cleaning with full config support.
    
    Args:
        wikitext: Raw Wikipedia markup
        config: Cleaning configuration from YAML
        
    Returns:
        Cleaned plain text or None if cleaning fails
    """
    if not wikitext:
        return None
    
    try:
        import mwparserfromhell
        
        # Fast pre-filter: check for Devanagari before expensive parsing
        if config.get("require_devanagari", True):
            if not re.search(r'[\u0900-\u097F]', wikitext):
                return None
        
        # Parse wikitext
        parsed = mwparserfromhell.parse(wikitext)
        
        # 1. Remove templates (includes infoboxes)
        if config.get("remove_templates", True):
            for template in list(parsed.filter_templates()):
                try:
                    # Special handling for infoboxes
                    if config.get("remove_infoboxes", True):
                        template_name = str(template.name).strip().lower()
                        if any(x in template_name for x in ['infobox', 'सूचनाकोश', 'जानकारीपेटिका']):
                            parsed.remove(template)
                            continue
                    
                    # Remove all templates if configured
                    parsed.remove(template)
                except:
                    pass
        
        # 2. Remove tables
        if config.get("remove_tables", True):
            for tag in list(parsed.filter_tags(matches=lambda n: n.tag == "table")):
                try:
                    parsed.remove(tag)
                except:
                    pass
        
        # 3. Remove references
        if config.get("remove_references", True):
            for tag in list(parsed.filter_tags(matches=lambda n: n.tag == "ref")):
                try:
                    parsed.remove(tag)
                except:
                    pass
        
        # 4. Remove external links
        if config.get("remove_external_links", True):
            for link in list(parsed.filter_external_links()):
                try:
                    parsed.remove(link)
                except:
                    pass
        
        # Convert to plain text
        text = parsed.strip_code()
        
        # 5. Remove categories
        if config.get("remove_categories", True):
            text = re.sub(r'\[\[Category:.*?\]\]', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\[\[श्रेणी:.*?\]\]', '', text, flags=re.IGNORECASE)
        
        # 6. Remove images/files
        if config.get("remove_images", True):
            text = re.sub(r'\[\[File:.*?\]\]', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\[\[Image:.*?\]\]', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\[\[चित्र:.*?\]\]', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\[\[फाइल:.*?\]\]', '', text, flags=re.IGNORECASE)
        
        # 7. Unicode normalization
        if config.get("normalize_unicode", True):
            text = unicodedata.normalize('NFC', text)
        
        # 8. Punctuation normalization
        if config.get("normalize_punctuation", False):
            for old, new in NEPALI_PUNCTUATION_MAP.items():
                text = text.replace(old, new)
        
        # 9. Whitespace normalization
        if config.get("normalize_whitespace", True):
            text = re.sub(r'\s+', ' ', text)
        
        text = text.strip()
        
        return text if text else None
        
    except Exception as e:
        LOGGER.debug(f"Cleaning failed: {e}")
        return None


# ============================================================================
# ARTICLE VALIDATION
# ============================================================================
def is_valid_article(
    title: str,
    wikitext: str,
    cleaned_text: Optional[str],
    namespace: int,
    config: Dict,
    skip_counter: SkipCounter
) -> bool:
    """
    Comprehensive article validation with all YAML checks.
    
    Args:
        title: Article title
        wikitext: Raw wikitext
        cleaned_text: Cleaned text (may be None)
        namespace: Wikipedia namespace
        config: Extraction configuration
        skip_counter: Skip counter
        
    Returns:
        True if valid
    """
    # 1. Namespace check
    allowed_ns = config.get("allowed_namespaces", [0])
    if namespace not in allowed_ns:
        skip_counter.increment("wrong_namespace")
        return False
    
    # 2. Redirect check (on raw wikitext)
    if config.get("skip_redirects", True):
        if is_redirect_raw(wikitext):
            skip_counter.increment("is_redirect")
            return False
    
    # 3. Cleaning failed
    if cleaned_text is None:
        skip_counter.increment("cleaning_failed")
        return False
    
    # 4. Disambiguation check
    if config.get("skip_disambiguation", True):
        disambig_markers = ["(बहुविकल्पी)", "बहुविकल्पी", "(disambiguation)"]
        if any(marker in title for marker in disambig_markers):
            skip_counter.increment("is_disambiguation")
            return False
        if any(marker in cleaned_text[:500] for marker in disambig_markers):
            skip_counter.increment("is_disambiguation")
            return False
    
    # 5. Length check
    min_len = config.get("min_article_length", 100)
    if len(cleaned_text) < min_len:
        skip_counter.increment("too_short")
        return False
    
    # Optional: max length check
    max_len = config.get("max_article_length", 0)
    if max_len > 0 and len(cleaned_text) > max_len:
        skip_counter.increment("too_long")
        return False
    
    # 6. Devanagari presence check
    if config.get("require_devanagari", True):
        if not re.search(r'[\u0900-\u097F]', cleaned_text):
            skip_counter.increment("no_devanagari")
            return False
    
    # 7. Devanagari ratio check
    min_ratio = config.get("min_devanagari_ratio", 0.0)
    if min_ratio > 0:
        ratio = calculate_devanagari_ratio(cleaned_text)
        if ratio < min_ratio:
            skip_counter.increment("low_devanagari_ratio")
            return False
    
    return True


# ============================================================================
# BATCH MANAGEMENT
# ============================================================================
def get_completed_batches(output_dir: Path, output_format: str) -> Set[int]:
    """Find completed batch files for restart safety."""
    if not output_dir.exists():
        return set()
    
    extension = ".jsonl" if output_format == "jsonl" else ".parquet"
    completed = set()
    
    for file in output_dir.glob(f"wikipedia_*{extension}"):
        try:
            batch_num = int(file.stem.split('_')[-1])
            completed.add(batch_num)
        except (ValueError, IndexError):
            LOGGER.warning(f"Unexpected filename: {file.name}")
    
    return completed


def write_batch_atomic(
    articles: List[Dict],
    output_dir: Path,
    batch_idx: int,
    output_format: str,
    metadata_fields: List[str]
):
    """
    Write batch atomically with metadata field filtering.
    
    Args:
        articles: Article dictionaries
        output_dir: Output directory
        batch_idx: Batch number
        output_format: "jsonl" or "parquet"
        metadata_fields: Fields to include in output
    """
    if not articles:
        return
    
    # Filter to only include specified metadata fields + text
    filtered_articles = []
    for article in articles:
        filtered = {"text": article["text"]}
        for field in metadata_fields:
            if field in article:
                filtered[field] = article[field]
        filtered_articles.append(filtered)
    
    if output_format == "jsonl":
        final_file = output_dir / f"wikipedia_{batch_idx:04d}.jsonl"
        temp_file = final_file.with_suffix(".jsonl.tmp")
        
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                for article in filtered_articles:
                    f.write(json.dumps(article, ensure_ascii=False) + '\n')
            temp_file.rename(final_file)
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise RuntimeError(f"Failed to write batch {batch_idx}: {e}")
    
    elif output_format == "parquet":
        import pandas as pd
        
        final_file = output_dir / f"wikipedia_{batch_idx:04d}.parquet"
        temp_file = final_file.with_suffix(".parquet.tmp")
        
        try:
            df = pd.DataFrame(filtered_articles)
            df.to_parquet(temp_file, index=False, engine='pyarrow')
            temp_file.rename(final_file)
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise RuntimeError(f"Failed to write batch {batch_idx}: {e}")
    
    else:
        raise ValueError(f"Unsupported format: {output_format}")
    
    LOGGER.info(f"  ✓ Batch {batch_idx}: {len(filtered_articles):,} articles → {final_file.name}")


# ============================================================================
# STREAMING XML PARSER - FIXED: BINARY MODE
# ============================================================================
def stream_wikipedia_articles(
    dump_path: Path,
    config: Dict,
    skip_counter: SkipCounter
) -> Iterator[Dict[str, Any]]:
    """
    Memory-efficient streaming parser for Wikipedia XML dumps.
    
    CRITICAL FIX: Opens bz2 file in BINARY mode for lxml compatibility.
    
    Args:
        dump_path: Path to .xml.bz2 dump
        config: Full configuration
        skip_counter: Skip counter
        
    Yields:
        Article dictionaries with text and metadata
    """
    LOGGER.info(f"Streaming from: {dump_path.resolve()}")
    
    # FIXED: Binary mode 'rb' instead of text mode 'rt'
    with bz2.open(dump_path, 'rb') as xml_file:
        context = etree.iterparse(xml_file, events=('end',), tag=f'{WIKI_NS}page')
        
        for event, page in context:
            try:
                # Extract namespace
                ns_elem = page.find(f'{WIKI_NS}ns')
                namespace = int(ns_elem.text) if ns_elem is not None else -1
                
                # Early namespace filter
                allowed_ns = config["extraction"].get("allowed_namespaces", [0])
                if namespace not in allowed_ns:
                    skip_counter.increment("wrong_namespace")
                    page.clear()
                    continue
                
                # Extract title
                title_elem = page.find(f'{WIKI_NS}title')
                if title_elem is None:
                    skip_counter.increment("no_text")
                    page.clear()
                    continue
                title = title_elem.text
                
                # Extract revision
                revision = page.find(f'{WIKI_NS}revision')
                if revision is None:
                    skip_counter.increment("no_revision")
                    page.clear()
                    continue
                
                # Get wikitext
                text_elem = revision.find(f'{WIKI_NS}text')
                if text_elem is None or text_elem.text is None:
                    skip_counter.increment("no_text")
                    page.clear()
                    continue
                wikitext = text_elem.text
                
                # Check redirect BEFORE cleaning
                if config["extraction"].get("skip_redirects", True):
                    if is_redirect_raw(wikitext):
                        skip_counter.increment("is_redirect")
                        page.clear()
                        continue
                
                # Get metadata
                page_id_elem = page.find(f'{WIKI_NS}id')
                page_id = page_id_elem.text if page_id_elem is not None else None
                
                timestamp_elem = revision.find(f'{WIKI_NS}timestamp')
                timestamp = timestamp_elem.text if timestamp_elem is not None else None
                
                # Clean wikitext
                cleaned_text = clean_wikitext(wikitext, config["cleaning"])
                
                # Paragraph filtering if configured
                if cleaned_text:
                    paragraphs = filter_paragraphs(cleaned_text, config["cleaning"])
                    if not paragraphs:
                        skip_counter.increment("all_paragraphs_filtered")
                        page.clear()
                        continue
                    cleaned_text = "\n\n".join(paragraphs)
                
                # Validate
                if is_valid_article(title, wikitext, cleaned_text, namespace, 
                                   config["extraction"], skip_counter):
                    yield {
                        "title": title,
                        "text": cleaned_text,
                        "page_id": page_id,
                        "timestamp": timestamp,
                        "namespace": namespace
                    }
                
            except Exception as e:
                LOGGER.warning(f"Error processing page: {e}")
            
            finally:
                # Critical memory cleanup
                page.clear()
                while page.getprevious() is not None:
                    del page.getparent()[0]
        
        del context


# ============================================================================
# MAIN EXTRACTION - FIXED: Added resume parameter (default False)
# ============================================================================
def extract_wikipedia(config_path: str = "wikipedia.yaml", resume: bool = False) -> Dict[str, Any]:
    """
    Extract Nepali Wikipedia articles with full config support.
    
    NOTE: Wikipedia dumps are CUMULATIVE (all articles up to download date).
    Running extraction multiple times on the same dump creates duplicates.
    Default behavior (resume=False) ALWAYS clears existing files first.
    
    Args:
        config_path: Path to configuration file
        resume: If True, resume from existing batches (for interrupted runs only).
                If False (DEFAULT), always clear output directory first to prevent duplicates.
        
    Returns:
        Extraction metrics dictionary
    """
    # Validate dependencies
    validate_dependencies()
    
    # Load and validate config
    config = load_config(config_path)
    validate_config(config)
    
    # Setup paths
    dump_dir = RAW_DIR / config["paths"]["raw_dump_dir"]
    dump_filename = config["paths"]["dump_filename"]
    dump_path = dump_dir / dump_filename

    output_dir = PROCESSED_DIR / config["paths"]["processed_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not dump_path.exists():
        raise FileNotFoundError(f"Wikipedia dump not found: {dump_path.resolve()}")
    
    output_format = config["output"]["format"]
    batch_size = config["output"]["batch_size"]
    max_articles = config["extraction"].get("max_articles", 0)
    progress_interval = config["logging"].get("progress_interval", 1000)
    metadata_fields = config["output"].get("metadata_fields", ["title", "page_id", "timestamp"])
    
    LOGGER.info("="*80)
    LOGGER.info("NEPALI WIKIPEDIA EXTRACTION - FULL PRODUCTION MODE")
    LOGGER.info("="*80)
    LOGGER.info(f"Input: {dump_path.resolve()}")
    LOGGER.info(f"Output: {output_dir.resolve()}")
    LOGGER.info(f"Format: {output_format}")
    LOGGER.info(f"Batch size: {batch_size:,}")
    LOGGER.info(f"Metadata fields: {', '.join(metadata_fields)}")
    if max_articles > 0:
        LOGGER.info(f"Max articles (testing): {max_articles:,}")
    LOGGER.info("-"*80)
    
    # Handle existing files based on resume flag
    if not resume:
        # ALWAYS clear existing batch files for fresh extraction (prevents duplicates)
        extension = ".jsonl" if output_format == "jsonl" else ".parquet"
        existing_files = list(output_dir.glob(f"wikipedia_*{extension}"))
        if existing_files:
            LOGGER.warning(f"⚠ Found {len(existing_files)} existing batch files")
            LOGGER.warning("⚠ Wikipedia dumps are CUMULATIVE - clearing old files to prevent duplicates")
            for file in existing_files:
                file.unlink()
            LOGGER.info(f"✓ Cleared {len(existing_files)} old batch files")
        LOGGER.info("✓ Starting FRESH extraction (resume=False)")
        completed_batches = set()
    else:
        # Check completed batches for resume (only for interrupted runs)
        LOGGER.warning("⚠ RESUME MODE ENABLED - will keep existing files")
        LOGGER.warning("⚠ Only use --resume to continue an interrupted run!")
        LOGGER.warning("⚠ Running on same dump twice creates duplicates in database!")
        completed_batches = get_completed_batches(output_dir, output_format)
        if completed_batches:
            LOGGER.info(f"Found {len(completed_batches)} completed batches - resuming from batch {max(completed_batches) + 1}")
        else:
            LOGGER.info("No existing batches found - starting fresh")
    
    LOGGER.info("-"*80)
    
    start_time = datetime.now()
    
    # Statistics
    total_processed = 0
    total_saved = 0
    skip_counter = SkipCounter()
    
    # Batch processing
    batch_articles = []
    batch_idx = max(completed_batches) + 1 if completed_batches else 1
    
    # Stream and process
    try:
        for article in stream_wikipedia_articles(dump_path, config, skip_counter):
            total_processed += 1
            batch_articles.append(article)
            
            # Write batch when full
            if len(batch_articles) >= batch_size:
                write_batch_atomic(batch_articles, output_dir, batch_idx, 
                                 output_format, metadata_fields)
                total_saved += len(batch_articles)
                batch_articles.clear()
                batch_idx += 1
            
            # Progress logging
            if total_processed % progress_interval == 0:
                LOGGER.info(
                    f"Processed: {total_processed:,} | "
                    f"Saved: {total_saved:,} | "
                    f"Skipped: {skip_counter.get_total():,} | "
                    f"Batch: {batch_idx}"
                )
            
            # Testing limit
            if max_articles > 0 and total_processed >= max_articles:
                LOGGER.info(f"Reached max_articles limit: {max_articles:,}")
                break
        
        # Write remaining
        if batch_articles:
            write_batch_atomic(batch_articles, output_dir, batch_idx, 
                             output_format, metadata_fields)
            total_saved += len(batch_articles)
        
    except Exception as e:
        LOGGER.error(f"Extraction failed: {e}")
        raise
    
    elapsed = (datetime.now() - start_time).total_seconds()
    total_skipped = skip_counter.get_total()
    
    # Results
    result = {
        "total_processed": total_processed,
        "total_saved": total_saved,
        "total_skipped": total_skipped,
        "batches_created": batch_idx - (max(completed_batches) if completed_batches else 0),
        "duration_seconds": elapsed,
        "skip_reasons": skip_counter.counts
    }
    
    LOGGER.info("="*80)
    LOGGER.info("EXTRACTION COMPLETE")
    LOGGER.info(f"Processed: {total_processed:,}")
    LOGGER.info(f"Saved: {total_saved:,}")
    LOGGER.info(f"Skipped: {total_skipped:,} ({total_skipped/max(total_processed,1)*100:.1f}%)")
    LOGGER.info(f"Batches: {result['batches_created']}")
    LOGGER.info(f"Time: {elapsed:.1f}s ({total_processed/max(elapsed,0.1):.1f} articles/sec)")
    LOGGER.info("")
    skip_counter.log_summary()
    LOGGER.info("="*80)
    
    return result


# ============================================================================
# MAIN
# ============================================================================
def main():
    """Main execution."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract Nepali Wikipedia articles",
        epilog="NOTE: Wikipedia dumps are cumulative. Always run WITHOUT --resume for new dumps to avoid duplicates."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="wikipedia.yaml",
        help="Path to configuration file (default: wikipedia.yaml)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from existing batches (ONLY for interrupted runs - causes duplicates otherwise!)"
    )
    
    args = parser.parse_args()
    
    try:
        result = extract_wikipedia(config_path=args.config, resume=args.resume)
        LOGGER.info(f"Extraction complete: {result}")
    except Exception as e:
        LOGGER.exception(f"Extraction failed: {e}")
        raise


if __name__ == "__main__":
    main()