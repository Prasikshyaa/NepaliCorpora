"""
robots.txt policy handler for web crawler.

Responsibilities:
- Fetch and cache robots.txt per domain
- Enforce crawl permissions
- Fail-open (if robots.txt unreachable, allow crawl)
"""

import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from scripts.utils.logger import get_logger


class RobotsPolicy:
    def __init__(self, allowed_domains: list[str], user_agent: str = "*"):
        self.allowed_domains = allowed_domains
        self.user_agent = user_agent

        self.logger = get_logger("robots", "ingestion")

        self._parsers: dict[str, RobotFileParser] = {}
        self._last_fetch: dict[str, float] = {}

        # Minimum seconds between robots.txt refetch
        self._cache_ttl = 24 * 3600  # 24 hours

    def _get_domain(self, url: str) -> str:
        return urlparse(url).netloc

    def _fetch_robots(self, domain: str) -> RobotFileParser | None:
        robots_url = f"https://{domain}/robots.txt"

        rp = RobotFileParser()
        rp.set_url(robots_url)

        try:
            rp.read()
            self.logger.info(f"[ROBOTS] Loaded {robots_url}")
            return rp
        except Exception as e:
            self.logger.warning(f"[ROBOTS FAIL] {robots_url} | {e}")
            return None

    def allowed(self, url: str) -> bool:
        """
        Check if URL is allowed to be crawled.
        Fail-open policy: allow if robots.txt unavailable.
        """

        domain = self._get_domain(url)

        # Restrict crawl strictly to allowed domains
        if not any(ad in domain for ad in self.allowed_domains):
            return False

        now = time.time()

        # Fetch or refresh robots.txt
        if (
            domain not in self._parsers
            or now - self._last_fetch.get(domain, 0) > self._cache_ttl
        ):
            rp = self._fetch_robots(domain)
            if rp:
                self._parsers[domain] = rp
                self._last_fetch[domain] = now
            else:
                # Fail-open if robots.txt not reachable
                return True

        rp = self._parsers.get(domain)
        if not rp:
            return True

        try:
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            return True
