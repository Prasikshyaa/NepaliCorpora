# Nepali Corpus Project: Automated Web Scraping and Text Processing Pipeline

**Author**: Prasikshya Karki
**Project Version**: 1.0.0
**Last Updated**: April 12, 2026
**Python Version**: 3.8+
**License**: Academic/Research Use

## Executive Summary

This project implements a comprehensive, production-grade system for building large-scale Nepali language corpora through automated web scraping, data processing, and quality assurance pipelines. The system collects news articles from major Nepali news websites, processes the text data, and prepares it for natural language processing research and machine learning applications.

## Project Scope and Objectives

### Primary Objectives
- Collect high-quality Nepali text data from diverse news sources
- Implement automated web crawling with respect for website policies
- Process and clean text data for NLP applications
- Ensure data quality through deduplication and validation
- Provide scalable infrastructure for ongoing data collection

### Target Data Sources
- Online Khabar
- Ekantipur
- Setopati
- Ratopati
- Ujyaalo Online
- Himal Khabar
- Gorkhapatra Online
- BBC Nepali
- Nepal Press

## System Architecture

### High-Level Architecture

```
Data Sources ──► Ingestion ──► Processing ──► Quality Assurance ──► Export
     │              │             │                │               │
   Websites     Web Crawling  Text Cleaning   Deduplication   Datasets
   (8+ sites)   (URL Discovery) (Unicode/Format) (Exact/Near)  (HuggingFace)
```

### Component Overview

#### 1. Ingestion Layer
- **Web Crawler**: Recursive URL discovery with pattern-based article identification
- **Content Scraper**: HTML parsing and text extraction from discovered URLs
- **State Management**: SQLite-based resumable crawling with progress tracking

#### 2. Processing Layer
- **Text Cleaning**: Unicode normalization, HTML removal, encoding fixes
- **Language Filtering**: Detection and isolation of Nepali language content
- **Text Segmentation**: Sentence and paragraph boundary detection

#### 3. Quality Assurance Layer
- **Exact Deduplication**: Removal of identical documents across the corpus
- **Near Deduplication**: Detection and removal of highly similar content
- **Validation**: Quality metrics and statistical analysis

#### 4. Export Layer
- **Format Conversion**: Standard dataset formats for ML applications
- **Packaging**: Compressed archives for distribution
- **Platform Integration**: HuggingFace Hub and Kaggle Datasets

## Automation Status

### Automated Components
- **Web Crawling**: Fully automated via Airflow DAGs and automation scripts
- **Content Scraping**: Automated pipeline for discovered URLs
- **Wikipedia Processing**: Automated download and extraction pipeline
- **Monitoring and Logging**: Comprehensive logging and error handling

### Manual Components
- **Text Preprocessing**: Requires manual execution of individual scripts
- **Deduplication**: Requires manual execution of deduplication scripts
- **Quality Validation**: Manual review and validation steps
- **Dataset Export**: Manual execution of export processes

### Automation Scripts Available
1. `scripts/automation/run_crawl_scrape.py`: Crawl and scrape pipeline
2. `scripts/automation/run_wiki_pipeline.py`: Wikipedia data pipeline
3. Airflow DAGs for scheduled execution

## Technical Implementation

### Core Technologies
- **Programming Language**: Python 3.8+
- **Web Scraping**: Requests, BeautifulSoup4, Selenium (for JavaScript sites)
- **Data Processing**: Pandas, NumPy
- **Database**: SQLite for crawl state management
- **Orchestration**: Apache Airflow for workflow management
- **Containerization**: Docker for deployment

### Key Features

#### Intelligent URL Discovery
- Recursive crawling with configurable depth limits
- Pattern-based article URL identification using regular expressions
- Domain-aware crawling with respect for robots.txt
- Rate limiting and polite crawling practices

#### Resumable Operations
- SQLite-based state persistence for crawl resumption
- Checkpoint-based processing for long-running operations
- Error recovery and partial failure handling

#### Quality Assurance
- Multi-level deduplication (exact and near-duplicate detection)
- Language identification and filtering
- Text normalization and cleaning
- Statistical validation and reporting

## Project Structure

```
Nepali_corpus_Project/
├── configs/                    # Configuration files
│   ├── websites.yaml          # Website crawling definitions
│   ├── dedup.yaml             # Deduplication settings
│   ├── preprocessing.yaml     # Text processing configuration
│   └── sources.yaml           # External data source definitions
├── scripts/                   # Python modules
│   ├── ingestion/             # Data collection
│   │   ├── webcrawler.py      # Main crawler implementation
│   │   ├── webscraper.py      # Content extraction
│   │   ├── url_normalizer.py  # URL standardization
│   │   └── robots.py          # Robots.txt compliance
│   ├── preprocessing/         # Text processing
│   │   ├── clean_text.py      # Text cleaning utilities
│   │   ├── language_filter.py # Language detection
│   │   └── segment_text.py    # Text segmentation
│   ├── deduplication/         # Quality assurance
│   │   ├── exact_dedup.py     # Exact duplicate removal
│   │   └── near_dedup.py      # Near-duplicate detection
│   └── utils/                 # Shared utilities
│       ├── config.py          # Configuration loading
│       ├── logger.py          # Logging system
│       └── paths.py           # Path management
├── data/                      # Data storage
│   ├── articles/              # Exported article URLs
│   ├── crawl_state/           # SQLite crawl databases
│   ├── raw/                   # Raw downloaded content
│   ├── processed/             # Cleaned text data
│   ├── metadata/              # Dataset statistics
│   └── deduplicated/          # Final quality datasets
├── logs/                      # Log files
│   ├── ingestion/             # Crawler logs
│   ├── preprocessing/         # Processing logs
│   └── deduplication/         # Deduplication logs
├── dags/                      # Apache Airflow DAGs
│   ├── crawl_scrape_dag.py    # Daily crawl and scrape pipeline
│   └── wikipedia_pipeline_dag.py # Wikipedia data pipeline
├── requirements.txt           # Python dependencies
└── README.md                  # This documentation
```

## Configuration Management

### Website Configuration (`configs/websites.yaml`)
Defines crawling parameters for each target website:

```yaml
sites:
  onlinekhabar:
    base_url: https://www.onlinekhabar.com
    start_urls:
      - https://www.onlinekhabar.com/
      - https://www.onlinekhabar.com/content/news
    allowed_domains:
      - onlinekhabar.com
      - www.onlinekhabar.com
    article_patterns:
      - "/\\d{4}/\\d{2}/\\d{7}/"  # Modern article URLs
      - "/\\d{4}/\\d{2}/\\d{6}/"  # Legacy article URLs
    exclude_patterns:
      - "/author/"
      - "/tag/"
      - "/search"
    max_depth: 6
    rate_limit_seconds: 2
    respect_robots_txt: false
```

### Processing Configuration
- Text cleaning parameters
- Language detection thresholds
- Deduplication sensitivity settings
- Export format specifications

## Data Processing Pipeline

### Phase 1: Data Ingestion
1. Load website configurations
2. Initialize crawl state databases
3. Execute recursive crawling with URL discovery
4. Export discovered article URLs to text files

### Phase 2: Content Collection
1. Load article URL lists
2. Fetch HTML content with error handling
3. Extract text content and metadata
4. Store raw content in structured format

### Phase 3: Text Processing
1. Clean HTML artifacts and normalize Unicode
2. Detect and filter Nepali language content
3. Segment text into sentences and paragraphs
4. Apply formatting and quality standards

### Phase 4: Quality Assurance
1. Perform exact deduplication across all sources
2. Apply near-duplicate detection algorithms
3. Generate quality metrics and statistics
4. Validate data integrity and completeness

### Phase 5: Data Export
1. Convert to standard ML dataset formats
2. Package datasets with metadata
3. Upload to data repositories
4. Generate documentation and usage examples

## Usage Instructions

### Prerequisites
- Python 3.8 or higher
- Docker and Docker Compose (for Airflow deployment)
- Virtual environment (recommended)

### Installation
```bash
# Clone repository
git clone <repository-url>
cd Nepali_corpus_Project

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

#### Automated Crawling
```bash
# Run automated crawl and scrape pipeline
python scripts/automation/run_crawl_scrape.py
```

#### Manual Component Execution

```bash
# Run web crawler only
python scripts/ingestion/webcrawler.py

# Process scraped content
python scripts/preprocessing/clean_text.py

# Perform deduplication
python scripts/deduplication/exact_dedup.py
python scripts/deduplication/near_dedup.py
```

### Airflow Deployment
```bash
# Start Airflow environment
docker-compose up -d

# Access Airflow UI at http://localhost:8081
# Enable and trigger DAGs for automated execution
```

### Database Management

#### Clearing Crawl State Before Deployment
Before deploying or running a fresh crawl, clear the existing crawl databases to ensure a clean start:

```bash
# Remove all crawl state databases
rm -rf data/crawl_state/*.sqlite

# Or on Windows PowerShell:
Remove-Item data/crawl_state/*.sqlite -Force

# Alternative: Remove specific site databases
rm data/crawl_state/onlinekhabar.sqlite
rm data/crawl_state/ekantipur.sqlite
# ... etc for other sites
```

**Important**: Clearing databases will reset crawl progress. The crawler will start fresh from the configured seed URLs. Keep databases if you want to resume previous crawl sessions.

## Monitoring and Maintenance

### Logging
- Comprehensive logging to `logs/` directory
- Separate log files for each processing phase
- Timestamped logs with configurable verbosity
- Error tracking and alerting capabilities

### Metrics and Statistics
- Crawl progress and success rates
- Data quality metrics (duplicate rates, language accuracy)
- Processing performance statistics
- Storage utilization tracking

### Maintenance Tasks
- Regular log rotation and cleanup
- Database optimization and vacuuming
- Configuration updates for new websites
- Dependency updates and security patches

## Quality Assurance and Validation

### Data Quality Metrics
- Language detection accuracy (>95%)
- Duplicate content removal (>80% reduction)
- Text cleanliness and formatting standards
- Unicode normalization compliance

### Validation Procedures
- Automated quality checks during processing
- Manual review of sample outputs
- Statistical analysis of dataset characteristics
- Cross-validation with external benchmarks

## Limitations and Future Work

### Current Limitations
- Deduplication requires manual execution
- Limited support for dynamic JavaScript-heavy websites
- Manual quality validation steps
- No real-time monitoring dashboard

### Planned Enhancements
- Full pipeline automation including deduplication
- Advanced JavaScript rendering support
- Real-time monitoring and alerting
- Multi-language support expansion
- Integration with additional data sources

## Dependencies and Requirements

### Core Dependencies
- requests: HTTP client library
- beautifulsoup4: HTML parsing
- lxml: XML processing
- PyYAML: Configuration file parsing
- selenium: JavaScript rendering (optional)
- pandas: Data manipulation
- apache-airflow: Workflow orchestration

### System Requirements
- Operating System: Linux, macOS, or Windows
- Memory: 8GB RAM minimum, 16GB recommended
- Storage: 50GB free space for data processing
- Network: Stable internet connection for web crawling

## Security and Ethical Considerations

### Responsible Crawling
- Respect for robots.txt directives
- Rate limiting to avoid server overload
- User agent identification for transparency
- Compliance with website terms of service

### Data Privacy
- No collection of personal user data
- Focus on publicly available news content
- Transparent data usage documentation
- Compliance with data protection regulations

## Contributing

### Development Guidelines
1. Follow PEP 8 style guidelines
2. Add comprehensive docstrings and type hints
3. Include unit tests for new functionality
4. Update documentation for configuration changes
5. Test changes across all supported platforms

### Code Review Process
- All changes require pull request review
- Automated testing must pass
- Documentation updates required
- Performance impact assessment

## License and Attribution

This project is developed for academic and research purposes in Nepali natural language processing. Please cite appropriately when using the collected datasets.

## Contact and Support

For technical issues, feature requests, or collaboration opportunities, please refer to the project documentation or contact the development team.

---

**Contact and Support**

For technical issues, feature requests, or collaboration opportunities, please refer to the project documentation or contact the development team.
4. Logs written to `logs/ingestion/`

---
*Report generated: April 12, 2026*</content>
<parameter name="filePath">c:\Nepali_corpus_Project\DEPLOYMENT_READINESS.md