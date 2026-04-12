#airflow dag file to crawl, scrape, preprocess and deduplicate data from websites
"""
Airflow DAG: Daily Nepali News Sites Crawl, Scrape, Preprocess & Deduplicate Pipeline

This DAG orchestrates the complete daily pipeline for collecting and processing news articles from Nepali news sites:
1. Crawl all sites configured in websites.yaml to discover new article URLs
2. Scrape discovered articles to extract content and metadata
3. Preprocess the scraped text (cleaning, normalization, segmentation)
4. Deduplicate the corpus using exact and near deduplication

Schedule: Daily at 2:00 AM Nepal Time (UTC+5:45)
Retries: 3 attempts with 5-minute delays
Timeout: Variable per task

Tasks:
- crawl_sites: Discover new article URLs from all configured sites
- scrape_sites: Extract content from discovered URLs
- preprocess_corpus: Clean and normalize scraped text
- deduplicate_corpus: Remove duplicate documents from the corpus
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ============================================================================
# CONFIGURATION
# ============================================================================

# Project paths (adjust if Airflow runs from different location)
PROJECT_ROOT = Path("/opt/airflow")  # Default Airflow project mount point
CRAWL_SCRAPE_SCRIPT = PROJECT_ROOT / "scripts" / "automation" / "run_crawl_scrape.py"
PREPROCESSING_SCRIPT = PROJECT_ROOT / "scripts" / "automation" / "run_preprocessing.py"
DEDUPLICATION_SCRIPT = PROJECT_ROOT / "scripts" / "automation" / "run_deduplication.py"

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],  # Update with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

dag = DAG(
    'nepali_news_crawl_scrape_preprocess_dedup',
    default_args=default_args,
    description='Daily crawl, scrape, preprocess and deduplicate Nepali news sites',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM (cron format)
    start_date=datetime(2026, 1, 29),
    catchup=False,  # Don't backfill historical runs
    tags=['nepali', 'news', 'crawl', 'scrape', 'preprocessing', 'deduplication', 'daily'],
    max_active_runs=1,  # Only one instance at a time
)

# ============================================================================
# TASK 1: CRAWL SITES
# ============================================================================

crawl_task = BashOperator(
    task_id='crawl_sites',
    bash_command=f'cd {PROJECT_ROOT} && python3 {CRAWL_SCRAPE_SCRIPT}',
    dag=dag,
    # Task-specific overrides
    retries=2,  # Fewer retries for crawl (it's resumable)
    retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(hours=4),  # Crawling can take time
    # Logging
    do_xcom_push=False,  # Don't push large outputs to XCom
)

# ============================================================================
# TASK 2: SCRAPE SITES
# ============================================================================

scrape_task = BashOperator(
    task_id='scrape_sites',
    bash_command=f'cd {PROJECT_ROOT} && python3 {CRAWL_SCRAPE_SCRIPT} scrape',
    dag=dag,
    # Task-specific overrides
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=6),  # Scraping can take time
    # Logging
    do_xcom_push=False,
)

# ============================================================================
# TASK 3: PREPROCESS CORPUS
# ============================================================================

preprocess_task = BashOperator(
    task_id='preprocess_corpus',
    bash_command=f'cd {PROJECT_ROOT} && python3 {PREPROCESSING_SCRIPT}',
    dag=dag,
    # Task-specific overrides
    retries=2,  # Fewer retries for preprocessing (deterministic)
    retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(hours=4),  # Preprocessing can take time
    # Logging
    do_xcom_push=False,
)

# ============================================================================
# TASK 4: DEDUPLICATE CORPUS
# ============================================================================

dedup_task = BashOperator(
    task_id='deduplicate_corpus',
    bash_command=f'cd {PROJECT_ROOT} && python3 {DEDUPLICATION_SCRIPT}',
    dag=dag,
    # Task-specific overrides
    retries=2,  # Fewer retries for deduplication (expensive to restart)
    retry_delay=timedelta(minutes=15),
    execution_timeout=timedelta(hours=8),  # Deduplication can be time-intensive
    # Logging
    do_xcom_push=False,
)

# ============================================================================
# TASK DOCUMENTATION
# ============================================================================

crawl_task.doc_md = """
### Crawl Sites Task

**Purpose**: Discover new article URLs from all configured Nepali news sites.

**What it does**:
1. Loads site configurations from `configs/websites.yaml`
2. Recursively crawls each site to discover article URLs
3. Saves discovered URLs to `data/raw/articles/{site}_urls.txt`
4. Tracks crawl state in SQLite databases for resumability

**Output**:
- Article URL lists in `data/raw/articles/`
- Crawl state in `data/raw/crawl_state/`
- Logs in `logs/ingestion/`

**Duration**: ~30 minutes to 2 hours depending on site sizes

**Resumable**: Yes - if interrupted, can resume from last crawled URL
"""

scrape_task.doc_md = """
### Scrape Sites Task

**Purpose**: Extract article content and metadata from discovered URLs.

**What it does**:
1. Loads article URLs from `data/raw/articles/`
2. Scrapes each URL to extract title, content, author, date
3. Cleans and preprocesses the text data
4. Saves structured data to Parquet format

**Output**:
- Processed articles in `data/processed/huggingface/`
- Scrape state in `data/raw/scrape_state/`
- Logs in `logs/ingestion/`

**Duration**: ~1-4 hours depending on number of articles

**Resumable**: Yes - uses SQLite state tracking
"""

preprocess_task.doc_md = """
### Preprocess Corpus Task

**Purpose**: Clean and normalize scraped text data for better quality.

**What it does**:
1. Loads scraped articles from `data/processed/huggingface/`
2. Applies text cleaning (Unicode normalization, HTML removal, formatting)
3. Filters content by language (Nepali text detection)
4. Segments text into sentences and paragraphs
5. Saves preprocessed data to Parquet format

**Techniques**:
- **Text Cleaning**: Regex-based removal of HTML, normalization of Unicode
- **Language Filtering**: Statistical language detection for Nepali content
- **Text Segmentation**: Sentence and paragraph boundary detection

**Output**:
- Preprocessed corpus in `data/processed/preprocessed/`
- Preprocessing statistics in `data/metadata/preprocessing_stats/`
- Logs in `logs/preprocessing/`

**Duration**: ~30 minutes to 2 hours depending on corpus size

**Resumable**: Yes - checkpoint-based processing
"""

dedup_task.doc_md = """
### Deduplicate Corpus Task

**Purpose**: Remove duplicate documents from the processed corpus.

**What it does**:
1. Performs exact deduplication using hash-based comparison
2. Performs near deduplication using MinHash + LSH for similarity detection
3. Generates deduplication statistics and reports

**Techniques**:
- **Exact Deduplication**: MD5 hash comparison for identical documents
- **Near Deduplication**: MinHash LSH with 85% similarity threshold

**Output**:
- Deduplicated corpus in `data/deduplicated/`
- Deduplication metadata in `data/metadata/`
- Logs in `logs/deduplication/`

**Duration**: ~2-6 hours depending on corpus size

**Resumable**: Yes - checkpoint-based processing
"""

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Define pipeline: crawl → scrape → preprocess → deduplicate
crawl_task >> scrape_task >> preprocess_task >> dedup_task

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Nepali News Crawl, Scrape & Deduplicate Pipeline

## Overview
This DAG automates the complete daily pipeline for collecting and processing news articles from Nepali news websites, including deduplication.

## Sites Covered
- Online Khabar
- Ekantipur
- Setopati
- Ratopati
- Ujyaalo Online
- Himal Khabar
- Gorkhapatra Online
- BBC Nepali
- Nepal Press

## Schedule
- **Frequency**: Daily
- **Time**: 2:00 AM Nepal Time
- **Duration**: 4-12 hours total

## Data Flow
```
websites.yaml → webcrawler.py → article URLs → webscraper.py → Parquet files → preprocessing → deduplication → clean corpus
```

## Pipeline Stages

### 1. Crawling (~30 min - 2 hours)
- Discover new article URLs from news sites
- Resume from last successful crawl
- Output: `data/raw/articles/{site}_urls.txt`

### 2. Scraping (~1-4 hours)
- Extract content from discovered URLs
- Clean and preprocess text
- Output: `data/processed/huggingface/*.parquet`

### 3. Preprocessing (~30 min - 2 hours)
- Clean and normalize scraped text
- Apply language filtering and segmentation
- Output: `data/processed/preprocessed/*.parquet`

### 4. Deduplication (~2-6 hours)
- Exact deduplication (hash-based)
- Near deduplication (MinHash + LSH, 85% threshold)
- Output: `data/deduplicated/`

## Outputs
- **URLs**: `data/raw/articles/{site}_urls.txt`
- **Articles**: `data/processed/huggingface/{site}/articles.parquet`
- **Preprocessed**: `data/processed/preprocessed/{site}/articles.parquet`
- **Deduplicated**: `data/deduplicated/document_level/`
- **Logs**: `logs/ingestion/`, `logs/preprocessing/`, `logs/deduplication/`

## Monitoring
- Check Airflow UI for task status
- Review logs at `logs/ingestion/`, `logs/preprocessing/`, `logs/deduplication/`
- Monitor metrics at `logs/metrics/`

## Error Handling
- **Retries**: 3 attempts with 5-minute delays
- **Resumable**: All stages support resume from interruption
- **State Tracking**: SQLite databases for progress tracking

## Dependencies
Each task depends on the successful completion of the previous task:
`crawl_sites` → `scrape_sites` → `preprocess_corpus` → `deduplicate_corpus`
"""