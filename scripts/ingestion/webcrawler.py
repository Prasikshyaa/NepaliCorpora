"""
Production-grade recursive web crawler for Nepali news sites.

Features:
- Fully recursive crawl until URL frontier exhausted
- Uses websites.yaml as seed input (ALL FIELDS SUPPORTED)
- Resumable (SQLite-based crawl state)
- URL normalization hook
- robots.txt enforcement hook
- JS rendering fallback via Selenium
- Transparent logging (uses existing logger.py)
- Rate-limited, domain-restricted
- Article pattern filtering
- Exclude pattern filtering
- Max depth tracking
"""

from __future__ import annotations

import re
import time
import sqlite3
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from scripts.utils.logger import get_logger
from scripts.utils import paths

# Future extensions (we will implement next)
from scripts.ingestion.url_normalizer import normalize_url
from scripts.ingestion.robots import RobotsPolicy

# Optional JS rendering
USE_SELENIUM = True
SELENIUM_SCROLL_PAUSE = 2.0
SELENIUM_MAX_SCROLLS = 10

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except ImportError:
    webdriver = None
    USE_SELENIUM = False


# ============================================================================
# URL NORMALIZATION (Built-in)
# ============================================================================

def normalize_url(url: str) -> str | None:
    """
    Normalize URL to canonical form.
    
    - Remove fragments (#section)
    - Lowercase scheme and netloc
    - Remove default ports
    - Sort query parameters
    - Remove tracking parameters
    - Remove trailing slash (except for homepage)
    """
    try:
        parsed = urlparse(url)
        
        # Skip non-HTTP(S) URLs
        if parsed.scheme not in ("http", "https"):
            return None
        
        # Lowercase scheme and netloc
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        
        # Remove default ports
        if netloc.endswith(":80") and scheme == "http":
            netloc = netloc[:-3]
        elif netloc.endswith(":443") and scheme == "https":
            netloc = netloc[:-4]
        
        # Remove www. prefix (optional - comment out if you want to keep it)
        # if netloc.startswith("www."):
        #     netloc = netloc[4:]
        
        path = parsed.path
        
        # Remove trailing slash (except for homepage)
        if path.endswith("/") and len(path) > 1:
            path = path[:-1]
        
        # Sort query parameters and remove tracking params
        if parsed.query:
            params = parse_qs(parsed.query, keep_blank_values=True)
            
            # Remove common tracking parameters
            tracking_params = {
                "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
                "fbclid", "gclid", "msclkid", "mc_cid", "mc_eid",
                "_ga", "_gl", "ref", "source"
            }
            
            filtered_params = {
                k: v for k, v in params.items() 
                if k.lower() not in tracking_params
            }
            
            # Sort for consistency
            sorted_query = urlencode(sorted(filtered_params.items()), doseq=True)
        else:
            sorted_query = ""
        
        # Reconstruct URL (no fragment)
        normalized = urlunparse((
            scheme,
            netloc,
            path,
            parsed.params,
            sorted_query,
            ""  # No fragment
        ))
        
        return normalized
    
    except Exception:
        return None


# ============================================================================
# ROBOTS.TXT POLICY (Built-in)
# ============================================================================

class RobotsPolicy:
    """Simple robots.txt checker with per-domain caching."""
    
    def __init__(self, allowed_domains: list[str]):
        self.allowed_domains = allowed_domains
        self.cache: dict[str, RobotFileParser] = {}
        self.cache_time: dict[str, float] = {}
        self.cache_ttl = 86400  # 24 hours
    
    def allowed(self, url: str, user_agent: str = "*") -> bool:
        """Check if URL is allowed by robots.txt."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            
            # Check cache
            current_time = time.time()
            if domain in self.cache:
                if current_time - self.cache_time[domain] < self.cache_ttl:
                    return self.cache[domain].can_fetch(user_agent, url)
            
            # Fetch and cache robots.txt
            robots_url = f"{parsed.scheme}://{domain}/robots.txt"
            rp = RobotFileParser()
            rp.set_url(robots_url)
            
            try:
                rp.read()
            except Exception:
                # If robots.txt unavailable, allow by default
                pass
            
            self.cache[domain] = rp
            self.cache_time[domain] = current_time
            
            return rp.can_fetch(user_agent, url)
        
        except Exception:
            # On any error, allow by default
            return True


# ============================================================================
# MAIN CRAWLER CLASS
# ============================================================================


class WebCrawler:
    def __init__(
        self,
        site_name: str,
        start_urls: list[str],
        allowed_domains: list[str],
        article_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        max_depth: int = 5,
        rate_limit_seconds: float = 1.0,
        respect_robots_txt: bool = True,
        requires_javascript: bool = False,
        user_agent: str | None = None,
        db_path: Path | None = None,
    ):
        self.site_name = site_name
        self.start_urls = start_urls
        self.allowed_domains = allowed_domains
        self.article_patterns = [re.compile(p) for p in (article_patterns or [])]
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.max_depth = max_depth
        self.rate_limit = rate_limit_seconds
        self.respect_robots_txt = respect_robots_txt
        self.requires_javascript = requires_javascript
        self.user_agent = user_agent or "NepaliCorpusBot/1.0"

        # Setup enhanced logging
        self.logger = self._setup_logger()

        self.db_path = db_path or paths.RAW_DIR / "crawl_state" / f"{site_name}.sqlite"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

        # Only initialize robots if respect_robots_txt is True
        self.robots = RobotsPolicy(allowed_domains) if respect_robots_txt else None

        # Initialize Selenium if required
        self.driver = self._init_selenium() if requires_javascript else None
    
    def _setup_logger(self):
        """Setup file + console logging for long crawls."""
        import logging
        from datetime import datetime
        
        log_dir = paths.PROJECT_ROOT / "logs" / "ingestion"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{self.site_name}_{timestamp}.log"
        
        # Setup logger
        logger = logging.getLogger(f"crawler.{self.site_name}")
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        logger.handlers = []
        
        # File handler (detailed INFO level)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        
        # Console handler (less verbose WARNING level)
        ch = logging.StreamHandler()
        ch.setLevel(logging.WARNING)
        ch.setFormatter(logging.Formatter(
            '%(levelname)s [%(name)s]: %(message)s'
        ))
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        logger.info(f"Logging to {log_file}")
        
        return logger

    # -------------------- DB --------------------

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()

        # Performance optimizations
        self.cur.execute("PRAGMA journal_mode=WAL")
        self.cur.execute("PRAGMA synchronous=NORMAL")
        self.cur.execute("PRAGMA cache_size=-64000")  # 64MB cache
        self.cur.execute("PRAGMA temp_store=MEMORY")

        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                depth INTEGER DEFAULT 0,
                is_article INTEGER DEFAULT 0,
                visited INTEGER DEFAULT 0,
                status_code INTEGER DEFAULT NULL,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        
        # Add indices for common queries
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_visited ON urls(visited)"
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_article ON urls(is_article)"
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_depth ON urls(depth)"
        )
        
        self.conn.commit()

    def _enqueue_url(self, url: str, depth: int = 0, is_article: bool = False):
        """Add URL to queue with depth and article status. NO COMMIT."""
        self.cur.execute(
            """
            INSERT OR IGNORE INTO urls (url, depth, is_article, visited) 
            VALUES (?, ?, ?, 0)
            """,
            (url, depth, int(is_article)),
        )
        # REMOVED: self.conn.commit()  # Commit controlled by crawl loop

    def _mark_visited(self, url: str):
        """Mark URL as visited. NO COMMIT."""
        self.cur.execute(
            "UPDATE urls SET visited = 1 WHERE url = ?",
            (url,),
        )
        # REMOVED: self.conn.commit()  # Commit controlled by crawl loop

    def _get_next_url(self) -> tuple[str, int] | None:
        """Get next unvisited URL with its depth."""
        row = self.cur.execute(
            "SELECT url, depth FROM urls WHERE visited = 0 ORDER BY depth ASC LIMIT 1"
        ).fetchone()
        return (row[0], row[1]) if row else None

    def _stats(self) -> dict:
        """Get crawl statistics."""
        stats = self.cur.execute(
            """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN visited = 1 THEN 1 ELSE 0 END) as visited,
                SUM(CASE WHEN visited = 0 THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN is_article = 1 THEN 1 ELSE 0 END) as articles
            FROM urls
            """
        ).fetchone()
        
        return {
            "total": stats[0],
            "visited": stats[1] or 0,
            "pending": stats[2] or 0,
            "articles": stats[3] or 0,
        }

    # -------------------- Selenium --------------------

    def _init_selenium(self):
        if not USE_SELENIUM or webdriver is None:
            self.logger.warning("Selenium requested but not available")
            return None

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        try:
            driver = webdriver.Chrome(options=options)
            self.logger.info("Selenium initialized successfully")
            return driver
        except Exception as e:
            self.logger.warning(f"Selenium unavailable: {e}")
            return None

    def _fetch_with_selenium(self, url: str) -> str | None:
        if not self.driver:
            return None

        try:
            self.driver.get(url)
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            for _ in range(SELENIUM_MAX_SCROLLS):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SELENIUM_SCROLL_PAUSE)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            return self.driver.page_source
        except Exception as e:
            self.logger.warning(f"[JS FAIL] {url} | {e}")
            return None

    # -------------------- URL filtering --------------------

    def _is_excluded(self, url: str) -> bool:
        """Check if URL matches any exclude pattern."""
        if not self.exclude_patterns:
            return False
        
        for pattern in self.exclude_patterns:
            if pattern.search(url):
                self.logger.debug(f"[EXCLUDED] {url} (pattern: {pattern.pattern})")
                return True
        return False

    def _is_article(self, url: str) -> bool:
        """Check if URL matches any article pattern."""
        if not self.article_patterns:
            # If no article patterns defined, treat all as potential articles
            return True
        
        for pattern in self.article_patterns:
            if pattern.search(url):
                return True
        return False

    def _allowed(self, url: str) -> bool:
        """Check if URL is allowed to be crawled."""
        parsed = urlparse(url)
        
        # Must be HTTP/HTTPS
        if not parsed.scheme.startswith("http"):
            return False
        
        # Must be in allowed domains (strict suffix matching)
        domain_ok = False
        for domain in self.allowed_domains:
            # Exact match or subdomain
            if parsed.netloc == domain or parsed.netloc.endswith("." + domain):
                domain_ok = True
                break
        
        if not domain_ok:
            return False
        
        # Must not match exclude patterns
        if self._is_excluded(url):
            return False
        
        return True

    # -------------------- Core crawl logic --------------------

    def _extract_links(self, html: str, base_url: str) -> list[str]:
        """Extract unique links from HTML."""
        soup = BeautifulSoup(html, "html.parser")
        links = set()  # Use set for automatic deduplication

        for a in soup.find_all("a", href=True):
            abs_url = urljoin(base_url, a["href"])
            norm = normalize_url(abs_url)
            if norm:
                links.add(norm)

        return list(links)

    def _fetch_html(self, url: str) -> tuple[str | None, int | None]:
        """
        Fetch HTML with fallback to Selenium if needed.
        Returns: (html, status_code)
        """
        try:
            headers = {"User-Agent": self.user_agent}
            r = requests.get(url, headers=headers, timeout=10)
            
            if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
                return r.text, r.status_code
            else:
                return None, r.status_code
        except Exception as e:
            self.logger.debug(f"[REQUESTS FAIL] {url} | {e}")
        
        # Fallback to Selenium if available
        if self.driver:
            self.logger.info(f"[JS FALLBACK] {url}")
            html = self._fetch_with_selenium(url)
            return html, 200 if html else None
        elif self.requires_javascript:
            self.logger.warning(f"[JS REQUIRED] {url} but Selenium unavailable")
        
        return None, None

    def crawl(self):
        """Main crawl loop with optimized commit strategy and graceful shutdown."""
        import signal
        
        # Graceful shutdown handler
        shutdown_requested = False
        def signal_handler(signum, frame):
            nonlocal shutdown_requested
            self.logger.warning("Shutdown signal received - finishing current URL...")
            shutdown_requested = True
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        self.logger.info("="*80)
        self.logger.info(f"Starting crawl: {self.site_name}")
        self.logger.info("="*80)
        self.logger.info(f"  Max depth: {self.max_depth}")
        self.logger.info(f"  Rate limit: {self.rate_limit}s")
        self.logger.info(f"  Robots.txt: {self.respect_robots_txt}")
        self.logger.info(f"  JavaScript: {self.requires_javascript}")
        self.logger.info(f"  Article patterns: {len(self.article_patterns)}")
        self.logger.info(f"  Exclude patterns: {len(self.exclude_patterns)}")
        self.logger.info(f"  Start URLs: {len(self.start_urls)}")

        # Seed URLs
        for url in self.start_urls:
            norm = normalize_url(url)
            if norm:
                self._enqueue_url(norm, depth=0, is_article=self._is_article(norm))
        
        # Initial commit for seed URLs
        self.conn.commit()

        processed = 0
        start_time = time.time()
        last_commit_time = time.time()
        last_commit_count = 0
        
        # Commit strategy: every N URLs OR every M seconds
        COMMIT_EVERY_N_URLS = 100
        COMMIT_EVERY_N_SECONDS = 300  # 5 minutes
        
        try:
            while not shutdown_requested:
                result = self._get_next_url()
                if not result:
                    break
                
                url, depth = result
                
                # Check max depth
                if depth > self.max_depth:
                    self.logger.debug(f"[MAX DEPTH] {url} (depth={depth})")
                    self._mark_visited(url)
                    continue

                # Check robots.txt
                if self.robots and not self.robots.allowed(url):
                    self.logger.info(f"[ROBOTS BLOCK] {url}")
                    self._mark_visited(url)
                    continue

                self.logger.info(f"[FETCH] {url} (depth={depth})")

                html, status_code = self._fetch_html(url)

                if html:
                    # Extract and enqueue child links
                    for link in self._extract_links(html, url):
                        if self._allowed(link):
                            is_article = self._is_article(link)
                            self._enqueue_url(link, depth=depth + 1, is_article=is_article)
                
                # Store status code
                if status_code:
                    self.cur.execute(
                        "UPDATE urls SET status_code = ? WHERE url = ?",
                        (status_code, url)
                    )

                self._mark_visited(url)
                processed += 1

                # Optimized commit strategy
                urls_since_commit = processed - last_commit_count
                time_since_commit = time.time() - last_commit_time
                
                if urls_since_commit >= COMMIT_EVERY_N_URLS or time_since_commit >= COMMIT_EVERY_N_SECONDS:
                    self.conn.commit()
                    last_commit_time = time.time()
                    last_commit_count = processed
                    self.logger.debug(f"[COMMIT] After {urls_since_commit} URLs")

                # Log progress every 10 pages
                if processed % 10 == 0:
                    stats = self._stats()
                    elapsed = time.time() - start_time
                    pages_per_sec = processed / elapsed if elapsed > 0 else 0
                    
                    self.logger.info(
                        f"[{self.site_name}] "
                        f"VISITED={stats['visited']:,} "
                        f"QUEUE={stats['pending']:,} "
                        f"ARTICLES={stats['articles']:,} "
                        f"({pages_per_sec:.2f} pages/sec)"
                    )

                time.sleep(self.rate_limit)
        
        except KeyboardInterrupt:
            self.logger.warning("KeyboardInterrupt - committing and shutting down...")
        
        finally:
            # Final commit
            self.conn.commit()
            
            stats = self._stats()
            elapsed = time.time() - start_time
            
            self.logger.info("="*80)
            self.logger.info(f"Crawl finished: {self.site_name}")
            self.logger.info(f"  Total URLs: {stats['total']:,}")
            self.logger.info(f"  Visited: {stats['visited']:,}")
            self.logger.info(f"  Articles found: {stats['articles']:,}")
            self.logger.info(f"  Time elapsed: {elapsed/3600:.2f} hours")
            self.logger.info(f"  Average speed: {stats['visited']/elapsed:.2f} pages/sec")
            self.logger.info("="*80)

            if self.driver:
                self.driver.quit()

    def export_articles(self, output_path: Path | None = None):
        """Export article URLs to text file."""
        if output_path is None:
            output_path = paths.RAW_DIR / "articles" / f"{self.site_name}_urls.txt"
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        rows = self.cur.execute(
            "SELECT url FROM urls WHERE is_article = 1 ORDER BY url"
        ).fetchall()
        
        with open(output_path, "w", encoding="utf-8") as f:
            for (url,) in rows:
                f.write(url + "\n")
        
        self.logger.info(f"Exported {len(rows)} article URLs to {output_path}")


# -------------------- CLI entry --------------------

def main():
    """
    Entry point: loads websites.yaml and crawls all sites sequentially.
    Supports both formats:
      1. Dict of dicts: sites: {onlinekhabar: {...}, ekantipur: {...}}
      2. List of dicts: websites: [{name: onlinekhabar, ...}, {name: ekantipur, ...}]
    """
    import yaml

    websites_yaml = paths.CONFIGS_DIR / "websites.yaml"
    with open(websites_yaml, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Support both "sites" and "websites" as top-level key
    sites_data = config.get("websites") or config.get("sites")
    
    if not sites_data:
        raise ValueError("No 'websites' or 'sites' key found in websites.yaml")

    # Normalize to list format
    if isinstance(sites_data, dict):
        # Format: sites: {onlinekhabar: {...}, ekantipur: {...}}
        sites_list = [
            {"name": site_name, **site_config} 
            for site_name, site_config in sites_data.items()
        ]
    else:
        # Format: websites: [{name: onlinekhabar, ...}, ...]
        sites_list = sites_data

    # Crawl each site
    for site_config in sites_list:
        site_name = site_config["name"]
        
        print(f"\n{'='*80}")
        print(f"Starting crawl: {site_name}")
        print(f"{'='*80}")
        
        crawler = WebCrawler(
            site_name=site_name,
            start_urls=site_config["start_urls"],
            allowed_domains=site_config["allowed_domains"],
            article_patterns=site_config.get("article_patterns"),
            exclude_patterns=site_config.get("exclude_patterns"),
            max_depth=site_config.get("max_depth", 5),
            rate_limit_seconds=site_config.get("rate_limit_seconds", 1.0),
            respect_robots_txt=site_config.get("respect_robots_txt", True),
            requires_javascript=site_config.get("requires_javascript", False),
            user_agent=site_config.get("user_agent"),
        )
        crawler.crawl()
        crawler.export_articles()
        crawler.conn.close()


if __name__ == "__main__":
    main()