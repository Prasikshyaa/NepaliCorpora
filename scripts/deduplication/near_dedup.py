# scripts/deduplication/near_dedup.py
"""
Near deduplication for Nepali text corpus using MinHash + LSH.
Optimized for large-scale datasets with:
- MinHash for efficient similarity estimation
- Locality Sensitive Hashing (LSH) for fast duplicate detection
- Configurable similarity thresholds
- Memory-efficient processing with streaming
- Parallel processing support
- Checkpoint/resume capability
"""

import hashlib
import sqlite3
import json
import pickle
from pathlib import Path
from datetime import datetime
from typing import Set, Dict, Any, List, Tuple, Iterator
import pandas as pd
import pyarrow.parquet as pq
from datasketch import MinHash, MinHashLSH
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import re
import unicodedata

from scripts.utils.paths import PROCESSED_DIR, DEDUP_DIR, METADATA_DIR
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# =========================
# LOGGER
# =========================
LOGGER = get_logger("deduplicate_near", log_type="deduplication")

# =========================
# CONFIG
# =========================
CONFIG = load_config("preprocessing.yaml")

INPUT_DIR = PROCESSED_DIR / "huggingface"
OUTPUT_DIR = DEDUP_DIR / "document_level"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEXT_COLUMN = "text"
BATCH_SIZE = CONFIG.get("dedup_batch_size", 50000)

# MinHash parameters
NUM_PERMUTATIONS = 128  # Number of permutations for MinHash
THRESHOLD = 0.85  # Similarity threshold (85%)
NUM_BANDS = 16  # Number of bands for LSH
ROWS_PER_BAND = NUM_PERMUTATIONS // NUM_BANDS

# Database paths
LSH_INDEX_PATH = METADATA_DIR / "near_dedup_lsh.pkl"
DUPLICATES_DB_PATH = METADATA_DIR / "near_duplicates.db"
CHECKPOINT_FILE = METADATA_DIR / "near_dedup_checkpoint.json"

# =========================
# NEPALI TEXT PREPROCESSING
# =========================

def preprocess_nepali_text(text: str) -> str:
    """
    Preprocess Nepali text for better deduplication.
    - Normalize Unicode
    - Remove extra whitespace
    - Keep Nepali characters and basic punctuation
    """
    if not isinstance(text, str):
        return ""

    # Normalize Unicode (handle different representations of same characters)
    text = unicodedata.normalize('NFC', text)

    # Remove control characters but keep Nepali and basic punctuation
    text = re.sub(r'[^\u0900-\u097F\u0980-\u09FF\s\.,!?;:\-\(\)\[\]{}"\'।॥]', ' ', text)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text

def create_minhash(text: str, num_perm: int = NUM_PERMUTATIONS) -> MinHash:
    """
    Create MinHash signature for text using shingles.
    """
    # Preprocess text
    text = preprocess_nepali_text(text)

    # Create shingles (n-grams of words)
    words = text.split()
    shingles = set()

    # Create word-level shingles (2-grams and 3-grams)
    for i in range(len(words)):
        # 2-gram
        if i < len(words) - 1:
            shingle = f"{words[i]} {words[i+1]}"
            shingles.add(shingle)

        # 3-gram
        if i < len(words) - 2:
            shingle = f"{words[i]} {words[i+1]} {words[i+2]}"
            shingles.add(shingle)

    # If no shingles, use character-level (fallback)
    if not shingles:
        chars = list(text)
        for i in range(len(chars)):
            if i < len(chars) - 2:
                shingle = ''.join(chars[i:i+3])
                shingles.add(shingle)

    # Create MinHash
    minhash = MinHash(num_perm=num_perm)
    for shingle in shingles:
        minhash.update(shingle.encode('utf-8'))

    return minhash

# =========================
# LSH INDEX MANAGEMENT
# =========================

class NearDedupIndex:
    """Manages MinHash LSH index for near deduplication."""

    def __init__(self, index_path: Path, threshold: float = THRESHOLD):
        self.index_path = index_path
        self.threshold = threshold
        self.lsh = None
        self.doc_ids = set()
        self._load_or_create_index()

    def _load_or_create_index(self):
        """Load existing LSH index or create new one."""
        if self.index_path.exists():
            try:
                with open(self.index_path, 'rb') as f:
                    data = pickle.load(f)
                    self.lsh = data['lsh']
                    self.doc_ids = data['doc_ids']
                LOGGER.info(f"Loaded existing LSH index with {len(self.doc_ids)} documents")
            except Exception as e:
                LOGGER.warning(f"Failed to load LSH index: {e}. Creating new index.")
                self._create_new_index()
        else:
            self._create_new_index()

    def _create_new_index(self):
        """Create new LSH index."""
        self.lsh = MinHashLSH(
            threshold=self.threshold,
            num_perm=NUM_PERMUTATIONS,
            params=(NUM_BANDS, ROWS_PER_BAND)
        )
        self.doc_ids = set()
        LOGGER.info("Created new LSH index")

    def save_index(self):
        """Save LSH index to disk."""
        data = {
            'lsh': self.lsh,
            'doc_ids': self.doc_ids
        }
        with open(self.index_path, 'wb') as f:
            pickle.dump(data, f)
        LOGGER.info(f"Saved LSH index with {len(self.doc_ids)} documents")

    def add_document(self, doc_id: str, minhash: MinHash) -> List[str]:
        """
        Add document to index and return list of similar documents.
        Returns list of document IDs that are similar (potential duplicates).
        """
        # Query for similar documents before adding
        similar_docs = self.lsh.query(minhash)

        # Add to index
        self.lsh.insert(doc_id, minhash)
        self.doc_ids.add(doc_id)

        return similar_docs

    def query_similar(self, minhash: MinHash) -> List[str]:
        """Query for documents similar to the given MinHash."""
        return self.lsh.query(minhash)

# =========================
# DUPLICATES DATABASE
# =========================

class DuplicatesDatabase:
    """SQLite database for storing duplicate relationships."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS duplicates (
                canonical_doc_id TEXT,
                duplicate_doc_id TEXT,
                similarity REAL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (canonical_doc_id, duplicate_doc_id)
            )
        """)
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_canonical ON duplicates(canonical_doc_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_duplicate ON duplicates(duplicate_doc_id)")
        self.conn.commit()

    def add_duplicate(self, canonical_id: str, duplicate_id: str, similarity: float):
        """Add a duplicate relationship."""
        self.cursor.execute("""
            INSERT OR REPLACE INTO duplicates
            (canonical_doc_id, duplicate_doc_id, similarity)
            VALUES (?, ?, ?)
        """, (canonical_id, duplicate_id, similarity))
        self.conn.commit()

    def get_duplicates_for_doc(self, doc_id: str) -> List[Tuple[str, float]]:
        """Get all duplicates for a document."""
        self.cursor.execute("""
            SELECT duplicate_doc_id, similarity FROM duplicates
            WHERE canonical_doc_id = ?
            UNION
            SELECT canonical_doc_id, similarity FROM duplicates
            WHERE duplicate_doc_id = ?
        """, (doc_id, doc_id))
        return self.cursor.fetchall()

    def count_duplicates(self) -> int:
        """Count total duplicate relationships."""
        self.cursor.execute("SELECT COUNT(*) FROM duplicates")
        return self.cursor.fetchone()[0]

# =========================
# CHECKPOINT MANAGEMENT
# =========================

class CheckpointManager:
    """Manages processing checkpoints for resumability."""

    def __init__(self, checkpoint_file: Path):
        self.checkpoint_file = checkpoint_file
        self.checkpoint = self._load_checkpoint()

    def _load_checkpoint(self) -> Dict[str, Any]:
        """Load checkpoint data."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                LOGGER.warning(f"Failed to load checkpoint: {e}")
        return {"processed_files": [], "last_batch_idx": 0}

    def save_checkpoint(self):
        """Save checkpoint data."""
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(self.checkpoint, f, indent=2, ensure_ascii=False)

    def is_processed(self, file_path: str) -> bool:
        """Check if file has been processed."""
        return file_path in self.checkpoint["processed_files"]

    def mark_processed(self, file_path: str):
        """Mark file as processed."""
        if file_path not in self.checkpoint["processed_files"]:
            self.checkpoint["processed_files"].append(file_path)
            self.save_checkpoint()

    def get_last_batch_idx(self) -> int:
        """Get last processed batch index."""
        return self.checkpoint.get("last_batch_idx", 0)

    def set_last_batch_idx(self, idx: int):
        """Set last processed batch index."""
        self.checkpoint["last_batch_idx"] = idx
        self.save_checkpoint()

    def clear(self):
        """Clear all checkpoint data."""
        self.checkpoint = {"processed_files": [], "last_batch_idx": 0}
        self.save_checkpoint()

# =========================
# PARALLEL PROCESSING
# =========================

def process_batch_parallel(batch_data: List[Tuple[str, str]]) -> List[Tuple[str, MinHash, List[str]]]:
    """
    Process a batch of documents in parallel to create MinHashes.
    Returns: [(doc_id, minhash, similar_docs), ...]
    """
    results = []
    for doc_id, text in batch_data:
        try:
            minhash = create_minhash(text)
            results.append((doc_id, minhash, []))
        except Exception as e:
            LOGGER.warning(f"Failed to process document {doc_id}: {e}")
            continue
    return results

# =========================
# MAIN DEDUPLICATION FUNCTION
# =========================

def deduplicate_near(resume: bool = True, use_parallel: bool = True, threshold: float = THRESHOLD):
    """
    Perform near deduplication using MinHash + LSH.

    Args:
        resume: Whether to resume from checkpoint
        use_parallel: Whether to use parallel processing
        threshold: Similarity threshold (0.0 to 1.0)
    """
    LOGGER.info("="*80)
    LOGGER.info("NEAR DEDUPLICATION - MINHASH + LSH")
    LOGGER.info(f"Similarity threshold: {threshold}")
    LOGGER.info(f"Parallel processing: {use_parallel}")
    LOGGER.info("="*80)

    # Initialize components
    index = NearDedupIndex(LSH_INDEX_PATH, threshold=threshold)
    duplicates_db = DuplicatesDatabase(DUPLICATES_DB_PATH)
    checkpoint_mgr = CheckpointManager(CHECKPOINT_FILE)

    # Find input files
    input_files = sorted(INPUT_DIR.rglob("*.parquet"))
    if not input_files:
        LOGGER.warning(f"No parquet files found in {INPUT_DIR}")
        return

    # Filter unprocessed files
    if resume:
        input_files = [f for f in input_files if not checkpoint_mgr.is_processed(str(f))]
        LOGGER.info(f"Resuming: {len(input_files)} files remaining")
    else:
        checkpoint_mgr.clear()
        index._create_new_index()  # Reset index
        LOGGER.info(f"Fresh start: {len(input_files)} files to process")

    if not input_files:
        LOGGER.info("All files already processed!")
        return

    # Statistics
    total_processed = 0
    total_duplicates = 0
    files_processed = 0

    start_time = datetime.now()

    # Determine CPU cores for parallel processing
    num_workers = min(mp.cpu_count(), 8) if use_parallel else 1
    LOGGER.info(f"Using {num_workers} worker processes")

    # Process each file
    for file_num, file_path in enumerate(input_files, 1):
        dataset_name = file_path.parent.name
        LOGGER.info(f"[{file_num}/{len(input_files)}] Processing {dataset_name}/{file_path.name}")

        try:
            # Read parquet file
            table = pq.read_table(file_path)
            df = table.to_pandas()

            if TEXT_COLUMN not in df.columns:
                LOGGER.warning(f"  ⚠️  Column '{TEXT_COLUMN}' not found, skipping")
                continue

            # Prepare batch data
            batch_data = []
            for idx, row in df.iterrows():
                doc_id = f"{dataset_name}_{file_path.stem}_{idx}"
                text = str(row[TEXT_COLUMN])
                if text.strip():  # Skip empty texts
                    batch_data.append((doc_id, text))

            if not batch_data:
                LOGGER.warning("  ⚠️  No valid texts found, skipping")
                continue

            LOGGER.info(f"  Documents: {len(batch_data):,}")

            # Process in batches
            batch_size = min(BATCH_SIZE, len(batch_data))

            for i in range(0, len(batch_data), batch_size):
                batch = batch_data[i:i + batch_size]
                LOGGER.info(f"  Processing batch {i//batch_size + 1}/{(len(batch_data) + batch_size - 1)//batch_size}")

                # Create MinHashes (parallel)
                if use_parallel and len(batch) > 100:
                    with ProcessPoolExecutor(max_workers=num_workers) as executor:
                        futures = []
                        sub_batch_size = max(1, len(batch) // num_workers)
                        for j in range(0, len(batch), sub_batch_size):
                            sub_batch = batch[j:j + sub_batch_size]
                            futures.append(executor.submit(process_batch_parallel, sub_batch))

                        batch_results = []
                        for future in futures:
                            batch_results.extend(future.result())
                else:
                    batch_results = process_batch_parallel(batch)

                # Process results and find duplicates
                batch_duplicates = 0
                for doc_id, minhash, _ in batch_results:
                    # Find similar documents
                    similar_docs = index.add_document(doc_id, minhash)

                    # Record duplicates
                    for similar_doc in similar_docs:
                        # Calculate actual similarity (optional - LSH gives approximate)
                        similarity = threshold  # Use threshold as approximation
                        duplicates_db.add_duplicate(similar_doc, doc_id, similarity)
                        batch_duplicates += 1

                total_duplicates += batch_duplicates
                LOGGER.info(f"    Duplicates found: {batch_duplicates}")

            # Mark file as processed
            checkpoint_mgr.mark_processed(str(file_path))
            files_processed += 1
            total_processed += len(batch_data)

            # Periodic index save
            if file_num % 5 == 0:
                index.save_index()
                LOGGER.info("  💾 Saved LSH index checkpoint")

        except Exception as e:
            LOGGER.error(f"  ❌ Failed to process {file_path}: {e}")
            continue

    # Final save
    index.save_index()

    # Summary
    duration = datetime.now() - start_time
    LOGGER.info("="*80)
    LOGGER.info("NEAR DEDUPLICATION COMPLETED")
    LOGGER.info(f"Files processed: {files_processed}")
    LOGGER.info(f"Documents processed: {total_processed:,}")
    LOGGER.info(f"Duplicate relationships found: {total_duplicates:,}")
    LOGGER.info(f"Duration: {duration}")
    LOGGER.info(f"Average docs/second: {total_processed / duration.total_seconds():.1f}")
    LOGGER.info("="*80)

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    # Run near deduplication
    deduplicate_near(resume=True, use_parallel=True, threshold=THRESHOLD)