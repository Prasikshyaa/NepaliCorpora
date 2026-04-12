"""
Production-grade article scraper for Nepali news sites.

Reads URLs from data/articles/*.txt (output from crawler)
Scrapes full article content and metadata
Saves to Parquet files: data/raw/scraped/{site}/articles.parquet

Features:
- Resumable (tracks scraped articles in SQLite)
- Site-specific content extraction
- Metadata extraction (title, author, date, etc.)
- Rate limiting per site
- Graceful error handling
- Progress tracking
- Batch writes to Parquet (efficient!)
- Partitioned by site
- Retry logic with exponential backoff
- Dead link handling
- Health metrics for monitoring
- Selenium on-demand (prevents memory leaks)
"""

from __future__ import annotations

import re
import time
import signal
import sqlite3
import hashlib
import requests
import pandas as pd
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from scripts.utils.logger import get_logger
from scripts.utils import paths


# ============================================================================
# SELENIUM (Optional, for JS-heavy sites)
# ============================================================================

USE_SELENIUM = True

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
except ImportError:
    webdriver = None
    USE_SELENIUM = False


# ============================================================================
# ARTICLE SCRAPER CLASS
# ============================================================================

class ArticleScraper:
    """Scrapes article content from discovered URLs and saves to Parquet."""
    
    def __init__(
        self,
        site_name: str,
        urls_file: Path,
        rate_limit_seconds: float = 2.0,
        user_agent: str = None,
        requires_javascript: bool = False,
        batch_size: int = 1000,  # Write to Parquet every N articles
        max_retries: int = 3,
    ):
        self.site_name = site_name
        self.urls_file = urls_file
        self.rate_limit = rate_limit_seconds
        self.user_agent = user_agent or "NepaliCorpusBot/1.0"
        self.requires_javascript = requires_javascript
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        # Setup logging
        self.logger = self._setup_logger()
        
        # Output directory
        self.output_dir = paths.RAW_DIR / "scraped" / site_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Parquet file path
        self.parquet_file = self.output_dir / "articles.parquet"
        
        # Database for tracking
        self.db_path = paths.RAW_DIR / "scrape_state" / f"{site_name}.sqlite"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        
        # Selenium (lazy initialization)
        self._driver_instance = None
        
        # Batch buffer for Parquet writes
        self.article_buffer: List[Dict[str, Any]] = []
        
        # Metrics for health monitoring
        self.metrics = {
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "retries": 0,
        }
        
        self.logger.info(f"Initialized scraper for {site_name}")
        self.logger.info(f"  Output: {self.parquet_file}")
        self.logger.info(f"  Batch size: {batch_size}")
        self.logger.info(f"  Max retries: {max_retries}")
        self.logger.info(f"  JavaScript: {requires_javascript}")
    
    def _normalize_text(self, text: str | None) -> str | None:
        """Normalize Unicode text for proper Nepali character handling.
        
        - Normalizes to NFC (Canonical Decomposition, followed by Canonical Composition)
        - Removes zero-width characters and invisible Unicode
        - Strips excessive whitespace
        - Ensures text is proper Python str
        """
        if text is None:
            return None
        
        # Ensure it's a string
        if not isinstance(text, str):
            text = str(text)
        
        # Normalize Unicode to NFC (standard for Nepali/Devanagari)
        text = unicodedata.normalize('NFC', text)
        
        # Remove zero-width spaces and other invisible characters
        # but keep Nepali-specific combining characters
        invisible_chars = [
            '\u200b',  # Zero-width space
            '\u200c',  # Zero-width non-joiner
            '\u200d',  # Zero-width joiner (keep for some scripts)
            '\ufeff',  # Zero-width no-break space (BOM)
            '\u00ad',  # Soft hyphen
        ]
        
        # Remove most invisible chars, but be careful with Devanagari
        for char in invisible_chars:
            if char != '\u200d':  # Keep zero-width joiner for Devanagari
                text = text.replace(char, '')
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text.strip() if text else None
    
    def _setup_logger(self):
        """Setup file + console logging."""
        import logging
        
        log_dir = paths.PROJECT_ROOT / "logs" / "scraping"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{self.site_name}_{timestamp}.log"
        
        logger = logging.getLogger(f"scraper.{self.site_name}")
        logger.setLevel(logging.INFO)
        logger.handlers = []
        
        # File handler
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.WARNING)
        ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def _init_db(self):
        """Initialize SQLite database for tracking scraped articles."""
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        
        # Performance optimizations
        self.cur.execute("PRAGMA journal_mode=WAL")
        self.cur.execute("PRAGMA synchronous=NORMAL")
        
        # Create table with basic schema first
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS scraped_articles (
                url TEXT PRIMARY KEY,
                article_id TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                file_path TEXT
            )
        """)
        
        # Migrate existing databases - add new columns if they don't exist
        self._migrate_database()
        
        # Create indexes
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_status ON scraped_articles(status)"
        )
        
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_retry ON scraped_articles(retry_count)"
        )
        
        self.conn.commit()
    
    def _migrate_database(self):
        """Migrate old database schema to new schema."""
        # Check which columns exist
        self.cur.execute("PRAGMA table_info(scraped_articles)")
        existing_columns = {row[1] for row in self.cur.fetchall()}
        
        # Add missing columns
        if "error_message" not in existing_columns:
            self.logger.info("Migrating database: adding error_message column")
            self.cur.execute(
                "ALTER TABLE scraped_articles ADD COLUMN error_message TEXT"
            )
        
        if "retry_count" not in existing_columns:
            self.logger.info("Migrating database: adding retry_count column")
            self.cur.execute(
                "ALTER TABLE scraped_articles ADD COLUMN retry_count INTEGER DEFAULT 0"
            )
        
        if "last_retry_at" not in existing_columns:
            self.logger.info("Migrating database: adding last_retry_at column")
            self.cur.execute(
                "ALTER TABLE scraped_articles ADD COLUMN last_retry_at TIMESTAMP"
            )
        
        self.conn.commit()
    
    def _get_selenium_driver(self):
        """Get or create Selenium driver (lazy initialization)."""
        if not USE_SELENIUM or webdriver is None:
            return None
        
        if self._driver_instance is None:
            try:
                options = Options()
                options.add_argument("--headless")
                options.add_argument("--disable-gpu")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-blink-features=AutomationControlled")
                
                self._driver_instance = webdriver.Chrome(options=options)
                self.logger.info("Selenium initialized successfully")
            except Exception as e:
                self.logger.warning(f"Selenium unavailable: {e}")
                return None
        
        return self._driver_instance
    
    def _close_selenium(self):
        """Close Selenium driver if open."""
        if self._driver_instance:
            try:
                self._driver_instance.quit()
                self.logger.info("Selenium driver closed")
            except Exception as e:
                self.logger.warning(f"Error closing Selenium: {e}")
            finally:
                self._driver_instance = None
    
    def _is_scraped(self, url: str) -> bool:
        """Check if URL already scraped successfully."""
        result = self.cur.execute(
            "SELECT status FROM scraped_articles WHERE url = ?",
            (url,)
        ).fetchone()
        
        if result is None:
            return False
        
        status = result[0]
        # Only skip if successfully scraped
        return status == "success"
    
    def _should_retry(self, url: str) -> bool:
        """Check if failed URL should be retried."""
        result = self.cur.execute(
            "SELECT retry_count, status FROM scraped_articles WHERE url = ?",
            (url,)
        ).fetchone()
        
        if result is None:
            return True  # First attempt
        
        retry_count, status = result
        
        # Don't retry dead links or successful scrapes
        if status in ["success", "dead_link"]:
            return False
        
        # Retry if under max retries
        return retry_count < self.max_retries
    
    def _mark_scraped(self, url: str, article_id: str, status: str, error_msg: str = None):
        """Mark article as scraped with error details. NO COMMIT."""
        # Get current retry count
        result = self.cur.execute(
            "SELECT retry_count FROM scraped_articles WHERE url = ?",
            (url,)
        ).fetchone()
        
        retry_count = result[0] + 1 if result else 0
        
        self.cur.execute("""
            INSERT OR REPLACE INTO scraped_articles 
            (url, article_id, status, file_path, error_message, retry_count, last_retry_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (url, article_id, status, str(self.parquet_file), error_msg, retry_count))
    
    def _flush_parquet_buffer(self):
        """Write buffered articles to Parquet file with proper UTF-8 encoding."""
        if not self.article_buffer:
            return
        
        self.logger.info(f"Writing {len(self.article_buffer)} articles to Parquet...")
        
        # Convert to DataFrame
        df_new = pd.DataFrame(self.article_buffer)
        
        # Ensure all string columns are proper str type
        string_columns = ['title', 'content', 'author', 'category', 'tags', 'site', 'url']
        for col in string_columns:
            if col in df_new.columns:
                df_new[col] = df_new[col].astype('string')
        
        # Append or create Parquet file
        if self.parquet_file.exists():
            # Read existing and append
            df_existing = pd.read_parquet(self.parquet_file)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_parquet(
                self.parquet_file,
                engine='pyarrow',
                compression='snappy',
                index=False,
                # Explicitly set string encoding for UTF-8
                use_deprecated_int96_timestamps=False,
                coerce_timestamps='ms',
                allow_truncated_timestamps=False,
            )
        else:
            # Create new file with explicit schema
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Define explicit schema with string types
            schema = pa.schema([
                ('article_id', pa.string()),
                ('url', pa.string()),
                ('scraped_at', pa.string()),
                ('site', pa.string()),
                ('title', pa.string()),
                ('content', pa.string()),
                ('author', pa.string()),
                ('published_date', pa.string()),
                ('modified_date', pa.string()),
                ('category', pa.string()),
                ('tags', pa.string()),
                ('images', pa.string()),
                ('word_count', pa.int64()),
                ('char_count', pa.int64()),
            ])
            
            # Convert to PyArrow table with schema
            table = pa.Table.from_pandas(df_new, schema=schema, preserve_index=False)
            
            # Write with explicit UTF-8 encoding
            pq.write_table(
                table,
                self.parquet_file,
                compression='snappy',
                use_deprecated_int96_timestamps=False,
                coerce_timestamps='ms',
                allow_truncated_timestamps=False,
            )
        
        # Clear buffer
        articles_written = len(self.article_buffer)
        self.article_buffer = []
        
        self.logger.info(f"Wrote {articles_written} articles to Parquet")
        
        return articles_written
    
    def _generate_article_id(self, url: str) -> str:
        """Generate unique article ID from URL."""
        # Try to extract ID from URL patterns
        patterns = [
            r'/(\d{6,})/',  # /123456/
            r'/story/(\d+)',  # /story/12345
            r'/news/(\d+)',   # /news/12345
            r'/articles/([a-z0-9]+)',  # /articles/c1x2y3z
            r'/(\d{4}/\d{2}/\d{2})/([^/]+)$',  # /2024/01/15/article-slug
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1).replace('/', '-')
        
        # Fallback: hash the URL
        return hashlib.md5(url.encode()).hexdigest()[:12]
    
    def _fetch_html(self, url: str) -> tuple[str | None, int | None, str | None]:
        """Fetch HTML content with retry logic and proper encoding handling.
        
        Returns:
            (html_content, status_code, error_message)
        """
        for attempt in range(self.max_retries):
            try:
                headers = {"User-Agent": self.user_agent}
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    # Ensure proper encoding for Nepali content
                    # requests library usually auto-detects, but we can help
                    if response.encoding is None or response.encoding.lower() not in ['utf-8', 'utf8']:
                        # Try to detect encoding from meta tags or default to UTF-8
                        response.encoding = response.apparent_encoding or 'utf-8'
                    
                    return response.text, response.status_code, None
                
                elif response.status_code in [429, 503]:  # Rate limit or service unavailable
                    wait_time = (attempt + 1) * 10
                    self.logger.warning(f"Status {response.status_code}, waiting {wait_time}s... (attempt {attempt+1}/{self.max_retries})")
                    time.sleep(wait_time)
                    self.metrics["retries"] += 1
                    continue
                
                elif response.status_code in [404, 410]:  # Not found or gone
                    return None, response.status_code, f"Dead link: {response.status_code}"
                
                else:
                    error_msg = f"HTTP {response.status_code}"
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"{error_msg}, retrying... (attempt {attempt+1}/{self.max_retries})")
                        time.sleep(5 * (attempt + 1))
                        self.metrics["retries"] += 1
                        continue
                    return None, response.status_code, error_msg
            
            except requests.exceptions.Timeout as e:
                error_msg = f"Timeout: {str(e)}"
                if attempt < self.max_retries - 1:
                    self.logger.warning(f"{error_msg}, retrying... (attempt {attempt+1}/{self.max_retries})")
                    time.sleep(5 * (attempt + 1))
                    self.metrics["retries"] += 1
                    continue
                return None, None, error_msg
            
            except requests.exceptions.RequestException as e:
                error_msg = f"Request failed: {str(e)}"
                if attempt < self.max_retries - 1:
                    self.logger.warning(f"{error_msg}, retrying... (attempt {attempt+1}/{self.max_retries})")
                    time.sleep(5 * (attempt + 1))
                    self.metrics["retries"] += 1
                    continue
                
                self.logger.debug(f"[REQUESTS FAIL] {url} | {error_msg}")
                break
        
        # Fallback to Selenium after retries
        if self.requires_javascript:
            self.logger.info(f"[JS FALLBACK] {url}")
            html = self._fetch_with_selenium(url)
            if html:
                return html, 200, None
            return None, None, "Selenium fetch failed"
        
        return None, None, "All retries exhausted"
    
    def _fetch_with_selenium(self, url: str) -> str | None:
        """Fetch with Selenium (for JS sites)."""
        driver = self._get_selenium_driver()
        if not driver:
            return None
        
        try:
            driver.get(url)
            # Wait for content to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
            time.sleep(2)  # Extra wait for dynamic content
            return driver.page_source
        except Exception as e:
            self.logger.warning(f"[SELENIUM FAIL] {url} | {e}")
            return None
    
    def _extract_content(self, html: str, url: str) -> Dict[str, Any]:
        """Extract article content and metadata with Unicode normalization."""
        soup = BeautifulSoup(html, "html.parser")
        
        article = {
            "url": url,
            "scraped_at": datetime.now().isoformat(),
            "site": self.site_name,
            "title": None,
            "content": None,
            "author": None,
            "published_date": None,
            "modified_date": None,
            "category": None,
            "tags": None,  # Will be string (comma-separated)
            "images": None,  # Will be string (comma-separated)
            "word_count": 0,
            "char_count": 0,
        }
        
        # Extract and normalize title
        article["title"] = self._normalize_text(self._extract_title(soup))
        
        # Extract and normalize content
        content = self._normalize_text(self._extract_article_content(soup))
        article["content"] = content
        
        if content:
            article["word_count"] = len(content.split())
            article["char_count"] = len(content)
        
        # Extract and normalize metadata
        article["author"] = self._normalize_text(self._extract_author(soup))
        article["published_date"] = self._extract_date(soup)  # Dates don't need normalization
        article["category"] = self._normalize_text(self._extract_category(soup, url))
        
        # Convert lists to comma-separated strings for Parquet with normalization
        tags = self._extract_tags(soup)
        article["tags"] = self._normalize_text(",".join(tags)) if tags else None
        
        images = self._extract_images(soup)
        article["images"] = ",".join(images) if images else None  # URLs don't need text normalization
        
        return article
    
    def _extract_title(self, soup: BeautifulSoup) -> str | None:
        """Extract article title."""
        # Try OpenGraph
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return og_title["content"].strip()
        
        # Try meta title
        meta_title = soup.find("meta", {"name": "title"})
        if meta_title and meta_title.get("content"):
            return meta_title["content"].strip()
        
        # Try <title> tag
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text().strip()
            # Remove site name suffix
            title = re.sub(r'\s*[\|\-]\s*[^\|]+$', '', title)
            return title
        
        # Try h1
        h1 = soup.find("h1")
        if h1:
            return h1.get_text().strip()
        
        return None
    
    def _extract_article_content(self, soup: BeautifulSoup) -> str | None:
        """Extract main article text."""
        # Try <article> tag
        article_tag = soup.find("article")
        if article_tag:
            # Remove scripts, styles, ads
            for tag in article_tag.find_all(["script", "style", "iframe", "nav", "aside"]):
                tag.decompose()
            
            # Get all paragraphs
            paragraphs = article_tag.find_all("p")
            text = "\n\n".join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
            
            if len(text) > 100:  # Minimum content length
                return text
        
        # Fallback: Find div with common class names
        content_classes = [
            "article-content", "entry-content", "post-content",
            "content", "main-content", "story-content",
            "news-content", "article-body"
        ]
        
        for class_name in content_classes:
            content_div = soup.find("div", class_=re.compile(class_name, re.I))
            if content_div:
                for tag in content_div.find_all(["script", "style", "iframe"]):
                    tag.decompose()
                
                paragraphs = content_div.find_all("p")
                text = "\n\n".join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
                
                if len(text) > 100:
                    return text
        
        # Last resort: All paragraphs
        paragraphs = soup.find_all("p")
        text = "\n\n".join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
        
        if len(text) > 100:
            return text
        
        return None
    
    def _extract_author(self, soup: BeautifulSoup) -> str | None:
        """Extract author name."""
        # Try meta tag
        author_meta = soup.find("meta", {"name": "author"})
        if author_meta and author_meta.get("content"):
            return author_meta["content"].strip()
        
        # Try article:author
        og_author = soup.find("meta", property="article:author")
        if og_author and og_author.get("content"):
            return og_author["content"].strip()
        
        # Try class-based
        author_classes = ["author", "author-name", "by-author", "article-author"]
        for class_name in author_classes:
            author_tag = soup.find(class_=re.compile(class_name, re.I))
            if author_tag:
                return author_tag.get_text().strip()
        
        return None
    
    def _extract_date(self, soup: BeautifulSoup) -> str | None:
        """Extract publication date."""
        # Try article:published_time
        published = soup.find("meta", property="article:published_time")
        if published and published.get("content"):
            return published["content"].strip()
        
        # Try time tag
        time_tag = soup.find("time")
        if time_tag:
            datetime_attr = time_tag.get("datetime")
            if datetime_attr:
                return datetime_attr
            return time_tag.get_text().strip()
        
        # Try meta date
        date_meta = soup.find("meta", {"name": "publish-date"})
        if date_meta and date_meta.get("content"):
            return date_meta["content"].strip()
        
        return None
    
    def _extract_category(self, soup: BeautifulSoup, url: str) -> str | None:
        """Extract article category."""
        # From URL
        url_parts = urlparse(url).path.split('/')
        if len(url_parts) > 1:
            # Common patterns: /politics/article or /category/politics
            for part in url_parts[1:3]:  # Check first 2 path segments
                if part and not part.isdigit() and len(part) > 2:
                    return part
        
        # From meta tag
        category_meta = soup.find("meta", property="article:section")
        if category_meta and category_meta.get("content"):
            return category_meta["content"].strip()
        
        return None
    
    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract article tags."""
        tags = []
        
        # Try meta keywords
        keywords = soup.find("meta", {"name": "keywords"})
        if keywords and keywords.get("content"):
            tags.extend([t.strip() for t in keywords["content"].split(",")])
        
        # Try article:tag
        article_tags = soup.find_all("meta", property="article:tag")
        for tag in article_tags:
            if tag.get("content"):
                tags.append(tag["content"].strip())
        
        return list(set(tags))[:10]  # Max 10 unique tags
    
    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract article images."""
        images = []
        
        # Try og:image
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            images.append(og_image["content"])
        
        # Try article images
        article_tag = soup.find("article")
        if article_tag:
            for img in article_tag.find_all("img")[:5]:  # Max 5 images
                src = img.get("src") or img.get("data-src")
                if src and src.startswith("http"):
                    images.append(src)
        
        return images
    
    def _log_health_metrics(self):
        """Log health metrics for monitoring."""
        metrics_dir = paths.PROJECT_ROOT / "logs" / "metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        metrics_file = metrics_dir / f"{self.site_name}_metrics.json"
        
        import json
        
        health_data = {
            "site": self.site_name,
            "timestamp": datetime.now().isoformat(),
            "metrics": self.metrics.copy(),
            "success_rate": self.metrics["succeeded"] / self.metrics["processed"] if self.metrics["processed"] > 0 else 0,
            "buffer_size": len(self.article_buffer),
            "parquet_file": str(self.parquet_file),
            "parquet_exists": self.parquet_file.exists(),
        }
        
        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(health_data, f, indent=2)
        
        self.logger.info(f"Health metrics logged to {metrics_file}")
    
    def scrape(self):
        """Main scraping loop."""
        # Load URLs
        if not self.urls_file.exists():
            self.logger.error(f"URLs file not found: {self.urls_file}")
            return
        
        with open(self.urls_file, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
        
        self.logger.info("="*80)
        self.logger.info(f"Starting scrape: {self.site_name}")
        self.logger.info(f"  Total URLs: {len(urls):,}")
        self.logger.info("="*80)
        
        # Setup graceful shutdown
        shutdown_requested = False
        def signal_handler(signum, frame):
            nonlocal shutdown_requested
            shutdown_requested = True
            self.logger.warning("Shutdown signal received...")
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        start_time = time.time()
        last_commit_time = time.time()
        last_commit_count = 0
        
        COMMIT_EVERY_N = 50
        COMMIT_EVERY_SECONDS = 300
        
        try:
            for url in urls:
                if shutdown_requested:
                    break
                
                # Check if already scraped successfully
                if self._is_scraped(url):
                    self.metrics["skipped"] += 1
                    continue
                
                # Check if should retry (for failed attempts)
                if not self._should_retry(url):
                    self.metrics["skipped"] += 1
                    continue
                
                article_id = self._generate_article_id(url)
                
                self.logger.info(f"[SCRAPE] {url}")
                
                # Fetch HTML with retry logic
                html, status_code, error_msg = self._fetch_html(url)
                
                if not html:
                    # Determine status based on error
                    if status_code in [404, 410]:
                        status = "dead_link"
                        self.logger.warning(f"[DEAD LINK] {url} (status={status_code})")
                    else:
                        status = "failed"
                        self.logger.warning(f"[FAIL] {url} | {error_msg}")
                    
                    self._mark_scraped(url, article_id, status, error_msg)
                    self.metrics["failed"] += 1
                    self.metrics["processed"] += 1
                    continue
                
                # Extract content
                article = self._extract_content(html, url)
                article["article_id"] = article_id
                
                # Check if content exists
                if not article["content"] or len(article["content"]) < 100:
                    self.logger.warning(f"[EMPTY] {url}")
                    self._mark_scraped(url, article_id, "empty", "Content too short or missing")
                    self.metrics["failed"] += 1
                    self.metrics["processed"] += 1
                    continue
                
                # Add to buffer
                self.article_buffer.append(article)
                self.metrics["succeeded"] += 1
                self.metrics["processed"] += 1
                
                # Mark as scraped in DB
                self._mark_scraped(url, article_id, "success")
                
                # Flush buffer to Parquet if batch size reached
                if len(self.article_buffer) >= self.batch_size:
                    self._flush_parquet_buffer()
                
                # Commit DB periodically
                if self.metrics["processed"] - last_commit_count >= COMMIT_EVERY_N or \
                   time.time() - last_commit_time >= COMMIT_EVERY_SECONDS:
                    self.conn.commit()
                    last_commit_time = time.time()
                    last_commit_count = self.metrics["processed"]
                
                # Progress
                if self.metrics["processed"] % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = self.metrics["processed"] / elapsed if elapsed > 0 else 0
                    success_rate = self.metrics["succeeded"] / self.metrics["processed"] * 100 if self.metrics["processed"] > 0 else 0
                    
                    self.logger.info(
                        f"[{self.site_name}] "
                        f"Processed={self.metrics['processed']:,} "
                        f"Success={self.metrics['succeeded']:,} ({success_rate:.1f}%) "
                        f"Failed={self.metrics['failed']:,} "
                        f"Skipped={self.metrics['skipped']:,} "
                        f"Retries={self.metrics['retries']} "
                        f"({rate:.1f}/sec)"
                    )
                
                # Rate limiting
                time.sleep(self.rate_limit)
        
        except KeyboardInterrupt:
            self.logger.warning("KeyboardInterrupt - committing...")
        
        finally:
            # Flush remaining articles in buffer
            if self.article_buffer:
                self.logger.info(f"Flushing {len(self.article_buffer)} remaining articles...")
                self._flush_parquet_buffer()
            
            # Final commit
            self.conn.commit()
            
            # Close Selenium
            self._close_selenium()
            
            # Log health metrics
            self._log_health_metrics()
            
            elapsed = time.time() - start_time
            success_rate = self.metrics["succeeded"] / self.metrics["processed"] * 100 if self.metrics["processed"] > 0 else 0
            
            self.logger.info("="*80)
            self.logger.info(f"Scraping complete: {self.site_name}")
            self.logger.info(f"  Processed: {self.metrics['processed']:,}")
            self.logger.info(f"  Succeeded: {self.metrics['succeeded']:,} ({success_rate:.1f}%)")
            self.logger.info(f"  Failed: {self.metrics['failed']:,}")
            self.logger.info(f"  Skipped: {self.metrics['skipped']:,}")
            self.logger.info(f"  Retries: {self.metrics['retries']}")
            self.logger.info(f"  Time: {elapsed/3600:.2f} hours")
            self.logger.info(f"  Rate: {self.metrics['processed']/elapsed:.2f} articles/sec")
            self.logger.info("="*80)
            
            self.conn.close()


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution - scrape all sites."""
    import yaml
    
    # Load config
    websites_yaml = paths.CONFIGS_DIR / "websites.yaml"
    with open(websites_yaml, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    sites = config.get("sites") or config.get("websites")
    
    if not sites:
        print("❌ No sites found in config")
        return
    
    # Convert to list format if dict
    if isinstance(sites, dict):
        sites_list = [{"name": name, **cfg} for name, cfg in sites.items()]
    else:
        sites_list = sites
    
    print("="*80)
    print("ARTICLE SCRAPER")
    print(f"Sites to scrape: {len(sites_list)}")
    print("="*80)
    
    # Scrape each site
    for idx, site_config in enumerate(sites_list, 1):
        site_name = site_config["name"]
        
        
        urls_file = paths.RAW_DIR / "articles" / f"{site_name}_urls.txt"
        
        if not urls_file.exists():
            print(f"\n⚠️  [{idx}/{len(sites_list)}] Skipping {site_name} - no URLs file")
            print(f"    Expected: {urls_file}")
            continue
        
        print(f"\n[{idx}/{len(sites_list)}] Scraping: {site_name}")
        
        try:
            scraper = ArticleScraper(
                site_name=site_name,
                urls_file=urls_file,
                rate_limit_seconds=site_config.get("rate_limit_seconds", 2.0),
                user_agent=site_config.get("user_agent"),
                requires_javascript=site_config.get("requires_javascript", False),
                max_retries=site_config.get("max_retries", 3),
            )
            scraper.scrape()
        
        except Exception as e:
            print(f"❌ Error scraping {site_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "="*80)
    print("ALL SITES SCRAPED")
    print(f"Output: {paths.RAW_DIR / 'scraped'}")
    print(f"Metrics: {paths.PROJECT_ROOT / 'logs' / 'metrics'}")
    print("="*80)


if __name__ == "__main__":
    main()