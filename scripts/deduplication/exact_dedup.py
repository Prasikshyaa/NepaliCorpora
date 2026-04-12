# scripts/deduplication/deduplicate_exact.py
"""
Production-grade exact deduplication for large-scale Nepali text corpus.
Optimized for 100+ GB datasets with:
- SQLite optimization for billions of hashes
- Memory-efficient processing
- Checkpoint/resume capability
- Parallel hash computation
- Progress tracking
"""
import hashlib
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Set, Dict, Any, List
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

from scripts.utils.paths import PROCESSED_DIR, DEDUP_DIR, METADATA_DIR
from scripts.utils.logger import get_logger
from scripts.utils.config import load_config

# =========================
# LOGGER
# =========================
LOGGER = get_logger("deduplicate_exact", log_type="deduplication")

# =========================
# CONFIG
# =========================
CONFIG = load_config("preprocessing.yaml")

INPUT_DIR = PROCESSED_DIR / "huggingface"
OUTPUT_DIR = DEDUP_DIR / "document_level"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEXT_COLUMN = "text"
BATCH_SIZE = CONFIG.get("dedup_batch_size", 50000)  # Smaller batches for memory
HASH_CHECK_BATCH = 10000  # Check hashes in batches of 10K

# Hash database with optimizations
HASH_DB_PATH = METADATA_DIR / "dedup_hashes.db"

# Checkpoint file
CHECKPOINT_FILE = METADATA_DIR / "dedup_checkpoint.json"

# =========================
# OPTIMIZED HASH DATABASE
# =========================
class OptimizedHashDatabase:
    """SQLite-based hash storage optimized for billions of entries."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._optimize_db()
        self._init_db()
    
    def _optimize_db(self):
        """Apply SQLite performance optimizations."""
        # Performance tuning for large datasets
        self.cursor.execute("PRAGMA journal_mode=WAL")  # Write-ahead logging
        self.cursor.execute("PRAGMA synchronous=NORMAL")  # Faster writes
        self.cursor.execute("PRAGMA cache_size=-2000000")  # 2GB cache
        self.cursor.execute("PRAGMA temp_store=MEMORY")  # Temp tables in memory
        self.cursor.execute("PRAGMA mmap_size=30000000000")  # Memory-mapped I/O
        self.conn.commit()
    
    def _init_db(self):
        """Initialize database schema."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS hashes (
                hash TEXT PRIMARY KEY,
                dataset_name TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITHOUT ROWID
        """)
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_dataset ON hashes(dataset_name)")
        self.conn.commit()
        
        # Analyze for query optimization
        self.cursor.execute("ANALYZE")
        self.conn.commit()
        
        LOGGER.info(f"Optimized hash database initialized at {self.db_path}")
    
    def count(self) -> int:
        """Get total number of unique hashes."""
        self.cursor.execute("SELECT COUNT(*) FROM hashes")
        return self.cursor.fetchone()[0]
    
    def exists_batch(self, hashes: List[str]) -> Set[str]:
        """Check multiple hashes at once (optimized for large batches)."""
        if not hashes:
            return set()
        
        # Use temporary table for large batch queries
        self.cursor.execute("CREATE TEMP TABLE IF NOT EXISTS temp_check (hash TEXT PRIMARY KEY)")
        self.cursor.execute("DELETE FROM temp_check")
        
        # Insert hashes to check
        self.cursor.executemany("INSERT OR IGNORE INTO temp_check VALUES (?)", [(h,) for h in hashes])
        
        # Join query for efficiency
        self.cursor.execute("""
            SELECT temp_check.hash 
            FROM temp_check 
            INNER JOIN hashes ON temp_check.hash = hashes.hash
        """)
        
        return {row[0] for row in self.cursor.fetchall()}
    
    def add_batch(self, hash_data: List[tuple]):
        """Add multiple hashes (optimized for bulk inserts)."""
        if not hash_data:
            return
        
        self.cursor.executemany(
            "INSERT OR IGNORE INTO hashes (hash, dataset_name) VALUES (?, ?)",
            hash_data
        )
        self.conn.commit()
    
    def vacuum(self):
        """Optimize database file size (run periodically)."""
        LOGGER.info("Running VACUUM to optimize database...")
        self.cursor.execute("VACUUM")
        self.conn.commit()
    
    def close(self):
        """Close database connection."""
        self.conn.close()

# =========================
# CHECKPOINT MANAGEMENT
# =========================
class CheckpointManager:
    """Manages processing checkpoints for resume capability."""
    
    def __init__(self, checkpoint_file: Path):
        self.checkpoint_file = checkpoint_file
        self.checkpoint = self._load()
    
    def _load(self) -> Dict:
        """Load checkpoint from file."""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {"processed_files": [], "last_batch_idx": 0}
    
    def save(self, processed_files: List[str], last_batch_idx: int):
        """Save checkpoint."""
        self.checkpoint = {
            "processed_files": processed_files,
            "last_batch_idx": last_batch_idx,
            "timestamp": datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.checkpoint, f, indent=2)
    
    def is_processed(self, file_path: str) -> bool:
        """Check if file was already processed."""
        return file_path in self.checkpoint.get("processed_files", [])
    
    def get_last_batch_idx(self) -> int:
        """Get last batch index."""
        return self.checkpoint.get("last_batch_idx", 0)
    
    def clear(self):
        """Clear checkpoint."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

# =========================
# PARALLEL HASH COMPUTATION
# =========================
def compute_hash(text: str) -> str:
    """Compute SHA256 hash of text."""
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()

def hash_batch_parallel(texts: List[str], n_workers: int = None) -> List[str]:
    """Compute hashes in parallel."""
    if n_workers is None:
        n_workers = max(1, mp.cpu_count() - 1)
    
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        return list(executor.map(compute_hash, texts))

# =========================
# STATISTICS
# =========================
def save_stats(stats: Dict[str, Any]):
    """Save deduplication statistics."""
    stats_dir = METADATA_DIR / "dedup_stats"
    stats_dir.mkdir(parents=True, exist_ok=True)
    
    stats_file = stats_dir / f"exact_dedup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    LOGGER.info(f"Statistics saved to {stats_file}")

# =========================
# MAIN DEDUPLICATION
# =========================
def deduplicate_exact(resume: bool = True, use_parallel: bool = True):
    """
    Perform exact deduplication across all processed datasets.
    
    Args:
        resume: Whether to resume from checkpoint
        use_parallel: Whether to use parallel hash computation
    """
    LOGGER.info("="*80)
    LOGGER.info("EXACT DEDUPLICATION - PRODUCTION MODE")
    LOGGER.info(f"Optimized for large-scale corpus (100+ GB)")
    LOGGER.info("="*80)
    
    # Initialize components
    hash_db = OptimizedHashDatabase(HASH_DB_PATH)
    checkpoint_mgr = CheckpointManager(CHECKPOINT_FILE)
    
    existing_hashes = hash_db.count()
    LOGGER.info(f"Existing unique hashes in database: {existing_hashes:,}")
    
    # Find all input files
    input_files = sorted(INPUT_DIR.rglob("*.parquet"))
    if not input_files:
        LOGGER.warning(f"No parquet files found in {INPUT_DIR}")
        return
    
    # Filter out already processed files if resuming
    if resume:
        input_files = [f for f in input_files if not checkpoint_mgr.is_processed(str(f))]
        LOGGER.info(f"Resuming: {len(input_files)} files remaining")
    else:
        checkpoint_mgr.clear()
        LOGGER.info(f"Fresh start: {len(input_files)} files to process")
    
    if not input_files:
        LOGGER.info("All files already processed!")
        return
    
    # Statistics
    total_input_rows = 0
    total_output_rows = 0
    total_duplicates = 0
    files_processed = 0
    files_failed = 0
    processed_file_paths = checkpoint_mgr.checkpoint.get("processed_files", [])
    
    start_time = datetime.now()
    batch_idx = checkpoint_mgr.get_last_batch_idx()
    
    # Process each file
    for file_num, file_path in enumerate(input_files, 1):
        dataset_name = file_path.parent.name
        
        LOGGER.info(f"[{file_num}/{len(input_files)}] {dataset_name}/{file_path.name}")
        
        try:
            # Read parquet (chunked for large files)
            table = pq.read_table(file_path)
            df = table.to_pandas()
            
            if TEXT_COLUMN not in df.columns:
                LOGGER.warning(f"  ⚠️  Column '{TEXT_COLUMN}' not found, skipping")
                files_failed += 1
                continue
            
            input_rows = len(df)
            total_input_rows += input_rows
            LOGGER.info(f"  Rows: {input_rows:,}")
            
            # Process in chunks to manage memory
            chunk_size = 100000
            unique_rows = []
            
            for chunk_start in range(0, len(df), chunk_size):
                chunk_end = min(chunk_start + chunk_size, len(df))
                df_chunk = df.iloc[chunk_start:chunk_end].copy()
                
                # Compute hashes (parallel if enabled and chunk is large)
                if use_parallel and len(df_chunk) > 10000:
                    hashes = hash_batch_parallel(df_chunk[TEXT_COLUMN].tolist())
                    df_chunk['hash'] = hashes
                else:
                    df_chunk['hash'] = df_chunk[TEXT_COLUMN].apply(compute_hash)
                
                # Check which hashes exist (in sub-batches for memory)
                all_existing = set()
                for i in range(0, len(df_chunk), HASH_CHECK_BATCH):
                    batch_hashes = df_chunk['hash'].iloc[i:i+HASH_CHECK_BATCH].tolist()
                    existing = hash_db.exists_batch(batch_hashes)
                    all_existing.update(existing)
                
                # Filter unique rows
                df_chunk['is_duplicate'] = df_chunk['hash'].isin(all_existing)
                df_unique = df_chunk[~df_chunk['is_duplicate']].copy()
                
                if len(df_unique) > 0:
                    # Add new hashes to database
                    hash_data = [(h, dataset_name) for h in df_unique['hash'].tolist()]
                    hash_db.add_batch(hash_data)
                    
                    # Prepare for output (drop internal columns)
                    df_unique = df_unique.drop(columns=['hash', 'is_duplicate'])
                    unique_rows.append(df_unique)
            
            # Combine all unique chunks
            if unique_rows:
                df_all_unique = pd.concat(unique_rows, ignore_index=True)
                duplicates = input_rows - len(df_all_unique)
                total_duplicates += duplicates
                
                LOGGER.info(f"  Unique: {len(df_all_unique):,} | Duplicates: {duplicates:,}")
                
                # Write in batches
                for i in range(0, len(df_all_unique), BATCH_SIZE):
                    batch_df = df_all_unique.iloc[i:i+BATCH_SIZE]
                    batch_idx += 1
                    
                    out_file = OUTPUT_DIR / f"deduped_{batch_idx:04d}.parquet"
                    batch_df.to_parquet(out_file, index=False, engine='pyarrow')
                    
                    total_output_rows += len(batch_df)
                    
                    if i == 0:  # Log first batch
                        LOGGER.info(f"  ✓ Writing batches starting from {batch_idx}")
            else:
                duplicates = input_rows
                total_duplicates += duplicates
                LOGGER.info(f"  All {duplicates:,} rows were duplicates")
            
            # Mark file as processed
            processed_file_paths.append(str(file_path))
            checkpoint_mgr.save(processed_file_paths, batch_idx)
            
            files_processed += 1
            
            # Periodic VACUUM (every 50 files)
            if files_processed % 50 == 0:
                hash_db.vacuum()
            
        except Exception as e:
            LOGGER.exception(f"  ✗ Error: {e}")
            files_failed += 1
            continue
    
    # Final optimization
    hash_db.vacuum()
    
    # Calculate statistics
    elapsed = (datetime.now() - start_time).total_seconds()
    dedup_rate = (total_duplicates / total_input_rows * 100) if total_input_rows > 0 else 0
    final_unique = hash_db.count()
    
    # Database size
    db_size_mb = HASH_DB_PATH.stat().st_size / (1024**2)
    
    # Prepare statistics
    stats = {
        "timestamp": datetime.now().isoformat(),
        "configuration": {
            "batch_size": BATCH_SIZE,
            "hash_check_batch": HASH_CHECK_BATCH,
            "parallel_processing": use_parallel
        },
        "input": {
            "files": len(input_files) + len(checkpoint_mgr.checkpoint.get("processed_files", [])),
            "rows": total_input_rows
        },
        "output": {
            "batches": batch_idx,
            "rows": total_output_rows
        },
        "deduplication": {
            "duplicates_found": total_duplicates,
            "dedup_rate_percentage": round(dedup_rate, 2),
            "total_unique_hashes": final_unique,
            "new_hashes_added": final_unique - existing_hashes
        },
        "processing": {
            "files_processed": files_processed,
            "files_failed": files_failed,
            "time_seconds": round(elapsed, 2),
            "rows_per_second": round(total_input_rows / elapsed, 2) if elapsed > 0 else 0
        },
        "storage": {
            "hash_db_size_mb": round(db_size_mb, 2),
            "hash_db_path": str(HASH_DB_PATH)
        },
        "paths": {
            "input": str(INPUT_DIR),
            "output": str(OUTPUT_DIR)
        }
    }
    
    save_stats(stats)
    
    # Log summary
    LOGGER.info("="*80)
    LOGGER.info("DEDUPLICATION COMPLETE")
    LOGGER.info(f"Input rows: {total_input_rows:,}")
    LOGGER.info(f"Output rows: {total_output_rows:,}")
    LOGGER.info(f"Duplicates removed: {total_duplicates:,} ({dedup_rate:.2f}%)")
    LOGGER.info(f"Total unique hashes: {final_unique:,}")
    LOGGER.info(f"Hash DB size: {db_size_mb:.1f} MB")
    LOGGER.info(f"Files processed: {files_processed}/{len(input_files)}")
    if files_failed > 0:
        LOGGER.warning(f"Files failed: {files_failed}")
    LOGGER.info(f"Output batches: {batch_idx}")
    LOGGER.info(f"Time: {elapsed:.1f}s ({total_input_rows/elapsed:.1f} rows/sec)")
    LOGGER.info(f"Output directory: {OUTPUT_DIR}")
    LOGGER.info("="*80)
    
    hash_db.close()
    
    # Clear checkpoint on success
    if files_failed == 0:
        checkpoint_mgr.clear()
        LOGGER.info("Checkpoint cleared (all files processed successfully)")

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    deduplicate_exact(resume=True, use_parallel=True)