#airflow dag file to crawl, scrape and deduplicate data from websites
"""
Airflow DAG: Daily Nepali News Sites Crawl, Scrape & Deduplicate Pipeline

This DAG orchestrates the daily collection and processing of news articles from Nepali news sites:
1. Crawl all sites configured in websites.yaml to discover new article URLs
2. Scrape discovered articles to extract content and metadata
3. Deduplicate the corpus using exact and near deduplication

Schedule: Daily at 2:00 AM Nepal Time (UTC+5:45)
Retries: 3 attempts with 5-minute delays
Timeout: Variable per task

Tasks:
- crawl_sites: Discover new article URLs from all configured sites
- scrape_sites: Extract content from discovered URLs
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
    'nepali_news_crawl_scrape_dedup',
    default_args=default_args,
    description='Daily crawl, scrape and deduplicate Nepali news sites',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM (cron format)
    start_date=datetime(2026, 1, 29),
    catchup=False,  # Don't backfill historical runs
    tags=['nepali', 'news', 'crawl', 'scrape', 'deduplication', 'daily'],
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
# TASK 3: DEDUPLICATE CORPUS
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

# Define pipeline: crawl → scrape → deduplicate
crawl_task >> scrape_task >> dedup_task

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
websites.yaml → webcrawler.py → article URLs → webscraper.py → Parquet files → deduplication → clean corpus
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

### 3. Deduplication (~2-6 hours)
- Exact deduplication (hash-based)
- Near deduplication (MinHash + LSH, 85% threshold)
- Output: `data/deduplicated/`

## Outputs
- **URLs**: `data/raw/articles/{site}_urls.txt`
- **Articles**: `data/processed/huggingface/{site}/articles.parquet`
- **Deduplicated**: `data/deduplicated/document_level/`
- **Logs**: `logs/ingestion/`, `logs/deduplication/`

## Monitoring
- Check Airflow UI for task status
- Review logs at `logs/ingestion/` and `logs/deduplication/`
- Monitor metrics at `logs/metrics/`

## Error Handling
- **Retries**: 3 attempts with 5-minute delays
- **Resumable**: All stages support resume from interruption
- **State Tracking**: SQLite databases for progress tracking

## Dependencies
Each task depends on the successful completion of the previous task:
`crawl_sites` → `scrape_sites` → `deduplicate_corpus`
"""
- **Resumability**: Crawl state preserved in SQLite
- **Partial failures**: Each site isolated - one failure doesn't affect others

## Manual Execution
```bash
# From project root
python3 scripts/automation/run_crawl_scrape.py
```

## Configuration
Edit `configs/websites.yaml` to:
- Add/remove sites
- Adjust rate limits
- Configure article patterns
- Set max crawl depth
"""