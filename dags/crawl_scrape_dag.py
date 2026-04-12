#airflow dag file to crawl and scrape data from websites
"""
Airflow DAG: Daily Nepali News Sites Crawl & Scrape Pipeline

This DAG orchestrates the daily collection of news articles from Nepali news sites:
1. Crawl all sites configured in websites.yaml to discover new article URLs
2. Scrape discovered articles to extract content and metadata

Schedule: Daily at 2:00 AM Nepal Time (UTC+5:45)
Retries: 3 attempts with 5-minute delays
Timeout: 2 hours per task

Tasks:
- crawl_sites: Discover new article URLs from all configured sites
- scrape_sites: Extract content from discovered URLs
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
AUTOMATION_SCRIPT = PROJECT_ROOT / "scripts" / "automation" / "run_crawl_scrape.py"

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
    'nepali_news_crawl_scrape',
    default_args=default_args,
    description='Daily crawl and scrape of Nepali news sites',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM (cron format)
    start_date=datetime(2026, 1, 29),
    catchup=False,  # Don't backfill historical runs
    tags=['nepali', 'news', 'crawl', 'scrape', 'daily'],
    max_active_runs=1,  # Only one instance at a time
)

# ============================================================================
# TASK 1: CRAWL SITES
# ============================================================================

crawl_task = BashOperator(
    task_id='crawl_sites',
    bash_command=f'cd {PROJECT_ROOT} && python3 {AUTOMATION_SCRIPT}',
    dag=dag,
    # Task-specific overrides
    retries=2,  # Fewer retries for crawl (it's resumable)
    retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(hours=4),  # Crawling can take time
    # Logging
    do_xcom_push=False,  # Don't push large outputs to XCom
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

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Single task - no dependencies (scraping runs separately after manual verification)
# In production, you might add: crawl_task >> scrape_task

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Nepali News Crawl & Scrape Pipeline

## Overview
This DAG automates the daily collection of news articles from Nepali news websites.

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
- **Duration**: 30 minutes - 2 hours

## Data Flow
```
websites.yaml → webcrawler.py → article URLs → webscraper.py → Parquet files
```

## Outputs
- **URLs**: `data/raw/articles/{site}_urls.txt`
- **Articles**: `data/raw/scraped/{site}/articles.parquet`
- **Logs**: `logs/ingestion/` and Airflow logs

## Monitoring
- Check Airflow UI for task status
- Review logs at `logs/ingestion/`
- Monitor metrics at `logs/metrics/`

## Error Handling
- **Retries**: 3 attempts with 5-minute delays
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