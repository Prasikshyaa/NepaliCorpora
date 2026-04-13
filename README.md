# Nepali Corpus Project

<sub>Author: Prasikshya Karki</sub><br>
<sub>GitHub: https://github.com/Prasikshyaa</sub><br>
<sub>LinkedIn: https://www.linkedin.com/in/prasikshya-karki-7882863a4/</sub><br>
<sub>Version: 2.0.0 &nbsp;|&nbsp; Updated: April 12, 2026 &nbsp;|&nbsp; Python: 3.8+ &nbsp;|&nbsp; Status: Production-Ready</sub>
---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture](#system-architecture)
3. [Technical Stack](#technical-stack)
4. [Project Structure](#project-structure)
5. [Methodologies](#methodologies)
6. [Installation & Setup](#installation--setup)
7. [Data Processing Pipeline](#data-processing-pipeline)
8. [Configuration](#configuration)
9. [Monitoring & Logs](#monitoring--logs)
10. [Quality Assurance](#quality-assurance)
11. [Security & Ethics](#security--ethics)
12. [Troubleshooting](#troubleshooting)
13. [Contributing](#contributing)
14. [License & Contact](#license--contact)

---

## Project Overview

A production-grade, fully automated system for building large-scale **Nepali language corpora** through intelligent web scraping, advanced text processing, and quality assurance pipelines.

### Objectives

1. Automate collection of high-quality Nepali text data
2. Implement production-grade web crawling and scraping
3. Process and clean data for NLP applications
4. Ensure data quality through language filtering and deduplication
5. Provide scalable infrastructure for ongoing collection

### Data Sources

| Source Type | Sites |
|-------------|-------|
| News Sites (9) | Online Khabar, Ekantipur, Setopati, Ratopati, Ujyaalo Online, Himal Khabar, Gorkhapatra Online, BBC Nepali, Nepal Press |
| Reference | Wikipedia Nepali edition |
| Supplementary | Kaggle pre-trained datasets |

### Key Statistics

| Metric | Value |
|--------|-------|
| News Sites Crawled | 9 |
| Articles Collected | 8+ million |
| Text Processing Success Rate | 95%+ |
| Language Filtering Accuracy | 95%+ |
| Deduplication Effectiveness | 60–70% total reduction |
| Processing Stages | 5 automated |

---

## System Architecture

### High-Level Data Flow

```
Raw Scraped Data (9 sites, 8M+ articles)
                ↓
┌─────────────────────────────────────────┐
│  STAGE 1: Text Cleaning                 │
│  • Unicode normalization                │
│  • HTML/URL/email removal               │
│  • Whitespace cleanup                   │
└─────────────────────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  STAGE 2: Language Filtering (95%+)     │
│  • Devanagari script detection          │
│  • Nepali word matching                 │
│  • Confidence scoring (>50%)            │
└─────────────────────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  STAGE 3: Text Segmentation             │
│  • Sentence splitting (Nepali punct.)   │
│  • Paragraph segmentation               │
│  • Length filtering                     │
└─────────────────────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  STAGE 4: Exact Deduplication           │
│  • MD5 hash comparison                  │
│  • Remove 100% identical docs           │
└─────────────────────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  STAGE 5: Near Deduplication            │
│  • MinHash LSH (85% similarity)         │
│  • Remove highly similar content        │
└─────────────────────────────────────────┘
                ↓
        Clean, High-Quality Nepali Corpus
```

### Component Architecture

```
Orchestration
├── Apache Airflow (scheduling + monitoring)
│   └── crawl_scrape_dag.py (daily: crawl→scrape→preprocess→dedup)
│
Automation
├── run_crawl_scrape.py
├── run_preprocessing.py
├── run_deduplication.py
└── run_wiki_pipeline.py
│
Modules
├── ingestion/         (URL discovery, HTML scraping)
├── preprocessing/     (clean_text, language_filter, segment_text)
├── deduplication/     (exact_dedup, near_dedup)
└── utils/             (config, logging, paths)
```

---

## Technical Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Python | 3.8+ |
| Web Scraping | Requests, BeautifulSoup4 | Latest |
| Data Processing | Pandas, NumPy, PyArrow | Latest |
| Orchestration | Apache Airflow | 2.8.1 |
| Database | PostgreSQL, SQLite | 14+ |
| Message Broker | Redis | 7.2+ |
| Containerization | Docker, Docker Compose | Latest |
| Config | YAML, PyYAML | Latest |
| Language Detection | Devanagari script + NLP | Custom |
| Deduplication | MinHash, LSH | datasketch |

### System Requirements

- **OS**: Linux, macOS, or Windows
- **Memory**: 8GB RAM minimum, 16GB recommended
- **Storage**: 50GB free space
- **Network**: Stable internet connection for web crawling

---

## Project Structure

```
Nepali_corpus_Project/
│
├── configs/
│   ├── websites.yaml          # 9 news sites config
│   ├── preprocessing.yaml     # Language filter thresholds
│   ├── dedup.yaml             # Dedup settings
│   ├── sources.yaml           # Data sources
│   └── wikipedia.yaml         # Wikipedia config
│
├── docker/
│   ├── Dockerfile             # Custom Airflow image with deps
│   ├── docker-compose.yml     # Full stack (Airflow + PostgreSQL + Redis)
│   ├── .dockerignore
│   ├── airflow-logs/
│   └── airflow-plugins/
│
├── dags/
│   ├── crawl_scrape_dag.py          # Daily pipeline
│   └── wikipedia_pipeline_dag.py    # Monthly pipeline
│
├── scripts/
│   ├── automation/
│   │   ├── run_crawl_scrape.py
│   │   ├── run_preprocessing.py
│   │   ├── run_deduplication.py
│   │   └── run_wiki_pipeline.py
│   ├── ingestion/
│   │   ├── webcrawler.py
│   │   ├── webscraper.py
│   │   ├── url_normalizer.py
│   │   ├── robots.py
│   │   └── wikipedia/
│   ├── preprocessing/
│   │   ├── clean_text.py
│   │   ├── language_filter.py
│   │   └── segment_text.py
│   ├── deduplication/
│   │   ├── exact_dedup.py
│   │   └── near_dedup.py
│   └── utils/
│       ├── config.py
│       ├── logger.py
│       └── paths.py
│
├── data/
│   ├── raw/                        # Raw URLs, crawl state
│   ├── processed/
│   │   ├── huggingface/            # Scraped (stage 0)
│   │   ├── preprocessed/           # After cleaning (stage 1)
│   │   ├── language_filtered/      # After filtering (stage 2)
│   │   ├── segmented/              # After segmentation (stage 3)
│   │   └── scrape_state/
│   ├── deduplicated/               # Final corpus
│   └── metadata/                   # Stats & logs
│
├── logs/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── deduplication/
│   ├── wikipedia/
│   └── airflow/
│
├── requirements.txt
├── setup_and_start.sh
├── README.md
├── DEPLOYMENT_READINESS.md
└── project_report.txt
```

---

## Methodologies

### 1. Language Filtering (Nepali Detection)

**Two-Level Detection:**

**Level 1 — Devanagari Script Check**
- Counts Unicode Devanagari characters (U+0900–U+097F)
- Calculates ratio to total text length
- Minimum threshold: 30% (configurable)

**Level 2 — Common Nepali Words**
- Matches against 200+ common Nepali words (छ, ले, को, मा, नेपाल, आज, etc.)
- Boosts confidence score when matches found
- Word-level validation

**Confidence Scoring:**
```
confidence = devanagari_ratio + (nepali_word_ratio × 0.3)
```

**Output Categories:**

| Category | Threshold | Interpretation |
|----------|-----------|----------------|
| High | ≥ 80% | Definitely Nepali |
| Medium | 60–80% | Likely Nepali |
| Low | 50–60% | Uncertain |
| Rejected | < 50% | Non-Nepali |

**Accuracy: 95%+** on real corpus data.

### 2. Text Segmentation

**Sentence Splitting**
- Splits on Nepali punctuation: `।`, `॥`, `!`, `?`, `…`
- Filters by word count (5–100 words per sentence)
- Preserves punctuation

**Paragraph Splitting**
- Splits on double newlines
- Minimum 50 characters per paragraph

Output includes sentence and paragraph counts as statistics per document.

### 3. Deduplication

**Exact Deduplication**
- MD5 hashing for content fingerprinting
- Removes 100% identical documents (~40–50% of corpus)

**Near Deduplication**
- MinHash LSH algorithm with 85% similarity threshold
- Removes highly similar but not identical content (~30–40% additional reduction)

---

## Installation & Setup

### Option 1: Docker (Recommended)

```bash
git clone https://github.com/Prasikshyaa/NepaliCorpora.git
cd Nepali_corpus_Project/docker
docker-compose build
docker-compose up -d
```

Access the Airflow web UI at `http://localhost:8081` (username: `airflow`, password: `airflow`).

**Services started:**
- Apache Airflow (port 8081)
- PostgreSQL database
- Redis cache

All dependencies are pre-installed in the Docker image.

### Option 2: Local Development

```bash
git clone https://github.com/Prasikshyaa/NepaliCorpora.git
cd Nepali_corpus_Project

python -m venv venv
source venv/bin/activate        # Windows: .\venv\Scripts\activate

pip install -r requirements.txt

python -m scripts.automation.run_preprocessing
```

---

## Data Processing Pipeline

| Stage | Description | Input | Output | Est. Time |
|-------|-------------|-------|--------|-----------|
| 1 | Text Cleaning | 2000+ raw parquet files | `data/processed/preprocessed/` | 30–60 min |
| 2 | Language Filtering | ~2000 cleaned files | `data/processed/language_filtered/` | 30–45 min |
| 3 | Text Segmentation | ~2000 filtered files | `data/processed/segmented/` | 20–30 min |
| 4 | Exact Deduplication | Segmented files | `data/deduplicated/` (partial) | 1–2 hrs |
| 5 | Near Deduplication | Stage 4 output | `data/deduplicated/` (final) | 2–4 hrs |

**Total estimated time:** 4–8 hours for full 8M+ article corpus.

Processing statistics are saved as JSON files in `data/metadata/preprocessing_stats/`.

### Common Commands

```bash
# Full preprocessing pipeline
python -m scripts.automation.run_preprocessing

# Language filtering only
python -m scripts.preprocessing.language_filter

# Docker: run inside container
docker-compose exec airflow-webserver python -m scripts.preprocessing.language_filter

# Docker: view logs
docker-compose logs -f

# Docker: open shell
docker-compose exec airflow-webserver /bin/bash
```

---

## Configuration

### `configs/preprocessing.yaml`

```yaml
# Language filtering
min_devanagari_ratio: 0.3     # Minimum 30% Devanagari characters
min_language_confidence: 0.5  # Minimum 50% confidence score

# Text processing
remove_html: true
normalize_unicode: true
min_document_length: 20

# Segmentation
sentence_split: true
paragraph_split: true
min_sentence_length: 5
```

Adjust `min_language_confidence` to control filtering sensitivity. Lowering it retains more borderline content; raising it enforces stricter Nepali-only filtering.

---

## Monitoring & Logs

### Log Locations

```
logs/preprocessing/
├── preprocessing.clean_text_*.log
├── preprocessing.language_filter_*.log
├── preprocessing.segment_text_*.log
└── automation.preprocessing_*.log
```

### Useful Commands

```bash
# Stream logs in real time
tail -f logs/preprocessing/*.log

# Check row counts and filtering rate
grep "Rows:" logs/preprocessing/*.log

# View language distribution breakdown
grep "Language Distribution" -A 5 logs/preprocessing/*.log
```

The Airflow web UI at `http://localhost:8081` provides DAG run history, task status, and execution logs for all scheduled pipelines.

---

## Quality Assurance

### Language Detection Test Cases

| Input | Expected Result |
|-------|----------------|
| Pure Nepali text | `is_nepali=True, confidence=0.81` |
| Mixed English-Nepali | `is_nepali=False` |
| Pure English | `is_nepali=False` |

### Deduplication Effectiveness

| Stage | Reduction |
|-------|-----------|
| Exact deduplication | 40–50% |
| Near deduplication (85%) | 30–40% additional |
| Combined | 60–70% total |

---

## Security & Ethics

### Responsible Crawling
- Respects `robots.txt` directives for all target sites
- Rate limiting applied to avoid server overload
- User-agent identification for transparency
- Compliant with each website's terms of service

### Data Privacy
- Collects only publicly available news content
- No collection of personal user data
- Transparent data usage documentation
- Compliant with data protection regulations

---

## Troubleshooting

**Language filter rejecting all rows**
```bash
# Lower confidence threshold temporarily
sed -i 's/min_language_confidence: 0.5/min_language_confidence: 0.3/' configs/preprocessing.yaml
python -m scripts.preprocessing.language_filter
```

**Docker not starting**
```bash
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

**Out of disk space**
```bash
du -sh data/*
rm logs/preprocessing/*.log.{1,2,3}   # Removes older rotated logs, keeps latest
```

---

## Contributing

1. Follow PEP 8 style guidelines
2. Add comprehensive docstrings and type hints
3. Include unit tests for new functionality
4. Update documentation for any configuration changes
5. Test changes across all supported platforms

All changes require a pull request review. Automated tests must pass and documentation must be updated before merging. Performance impact should be assessed for pipeline-affecting changes.

---

## Planned Enhancements

- Real-time monitoring and alerting dashboard
- Multi-language support expansion
- Integration with additional data sources

---

## License & Contact

Developed for academic and research purposes in Nepali natural language processing. Please cite appropriately when using the collected datasets.

**Repository**: https://github.com/Prasikshyaa/NepaliCorpora

**Contact**: prasikshyakarki@gmail.com — for technical issues, feature requests, or collaboration.

---

*Last updated: April 12, 2026*