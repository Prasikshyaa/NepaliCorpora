"""
Airflow DAG: Monthly Nepali Wikipedia Dump Download & Extraction Pipeline

This DAG orchestrates the monthly collection of Nepali Wikipedia articles:
1. Download latest Wikipedia dump from Wikimedia
2. Extract and clean articles to Parquet format

Schedule: Monthly on the 1st at 3:00 AM Nepal Time (UTC+5:45)
Retries: 3 attempts with 10-minute delays
Timeout: 4 hours per task

Tasks:
- download_wiki_dump: Download compressed XML dump from Wikimedia
- extract_wiki_articles: Parse XML, clean markup, extract articles
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
DOWNLOAD_SCRIPT = PROJECT_ROOT / "scripts" / "ingestion" / "wikipedia" / "download_dump.py"
EXTRACT_SCRIPT = PROJECT_ROOT / "scripts" / "ingestion" / "wikipedia" / "extract_nepali.py"

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],  # Update with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=4),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

dag = DAG(
    'nepali_wikipedia_pipeline',
    default_args=default_args,
    description='Monthly download and extraction of Nepali Wikipedia',
    schedule_interval='0 3 1 * *',  # Monthly on 1st at 3:00 AM (cron format)
    start_date=datetime(2026, 1, 1),
    catchup=False,  # Don't backfill historical runs
    tags=['nepali', 'wikipedia', 'monthly', 'corpus'],
    max_active_runs=1,  # Only one instance at a time
)

# ============================================================================
# TASK 1: DOWNLOAD WIKIPEDIA DUMP
# ============================================================================

download_task = BashOperator(
    task_id='download_wiki_dump',
    bash_command=f'cd {PROJECT_ROOT} && python3 {DOWNLOAD_SCRIPT}',
    dag=dag,
    # Task-specific overrides
    retries=5,  # More retries for downloads (network issues common)
    retry_delay=timedelta(minutes=15),
    execution_timeout=timedelta(hours=6),  # Downloads can be slow
    # Logging
    do_xcom_push=False,
)

# ============================================================================
# TASK 2: EXTRACT ARTICLES
# ============================================================================

extract_task = BashOperator(
    task_id='extract_wiki_articles',
    bash_command=f'cd {PROJECT_ROOT} && python3 {EXTRACT_SCRIPT}',
    dag=dag,
    # Task-specific overrides
    retries=3,
    retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(hours=4),
    # Logging
    do_xcom_push=False,
)

# ============================================================================
# TASK DOCUMENTATION
# ============================================================================

download_task.doc_md = """
### Download Wikipedia Dump Task

**Purpose**: Download latest Nepali Wikipedia dump from Wikimedia servers.

**What it does**:
1. Connects to Wikimedia dump server
2. Downloads `newiki-latest-pages-articles.xml.bz2` (~500 MB compressed)
3. Supports resume on network interruption
4. Verifies download completion

**Output**:
- Wikipedia dump: `data/raw/wikipedia/latest/newiki-latest-pages-articles.xml.bz2`
- Logs: `logs/wikipedia/`

**Duration**: ~10-30 minutes depending on network speed

**Resumable**: Yes - supports HTTP range requests for resume
"""

extract_task.doc_md = """
### Extract Wikipedia Articles Task

**Purpose**: Parse XML dump and extract clean Nepali article text.

**What it does**:
1. Streams through compressed XML dump (memory-efficient)
2. Removes Wikipedia markup (templates, tables, references, etc.)
3. Filters by namespace (main articles only)
4. Validates Devanagari content
5. Exports to Parquet batches

**Output**:
- Article batches: `data/processed/wikipedia/wikipedia_NNNN.parquet`
- Metadata: title, page_id, timestamp
- Logs: `logs/wikipedia/`

**Duration**: ~15-45 minutes for ~150,000 articles

**Resumable**: Yes - skips already-processed batches
"""

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Sequential execution: download → extract
download_task >> extract_task

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Nepali Wikipedia Pipeline

## Overview
This DAG automates the monthly download and extraction of Nepali Wikipedia articles.

## Data Source
- **Source**: Wikimedia Foundation dumps (dumps.wikimedia.org)
- **Language**: Nepali (newiki)
- **Size**: ~500 MB compressed, ~2 GB uncompressed
- **Articles**: ~150,000+ articles

## Schedule
- **Frequency**: Monthly
- **Day**: 1st of each month
- **Time**: 3:00 AM Nepal Time
- **Duration**: 30 minutes - 2 hours

## Data Flow
```
Wikimedia → download_dump.py → XML dump → extract_nepali.py → Parquet batches
```

## Outputs
- **Raw dump**: `data/raw/wikipedia/latest/newiki-latest-pages-articles.xml.bz2`
- **Processed articles**: `data/processed/wikipedia/wikipedia_*.parquet`
- **Logs**: `logs/wikipedia/`

## Article Processing
- **Cleaning**: Removes templates, tables, references, categories
- **Filtering**: Main namespace only, minimum length 100 chars
- **Validation**: Requires 30%+ Devanagari characters
- **Format**: Parquet (efficient columnar storage)

## Monitoring
- Check Airflow UI for task status
- Review logs at `logs/wikipedia/`
- Verify output at `data/processed/wikipedia/`

## Error Handling
- **Retries**: 3-5 attempts with 10-15 minute delays
- **Resumability**: Download supports resume, extraction skips completed batches
- **Timeouts**: 4-6 hours per task

## Manual Execution
```bash
# From project root
python3 scripts/automation/run_wiki_pipeline.py

# Or individual steps:
python3 scripts/ingestion/wikipedia/download_dump.py
python3 scripts/ingestion/wikipedia/extract_nepali.py
```

## Configuration
Edit `configs/wikipedia.yaml` to:
- Adjust filtering thresholds
- Configure batch sizes
- Set extraction parameters
- Enable/disable cleaning options

## Storage Requirements
- **Raw dump**: ~500 MB
- **Processed articles**: ~300-500 MB
- **Logs**: ~10 MB per run
- **Total**: ~1 GB per monthly run
"""