"""
URL normalization utilities for production web crawling.

Responsibilities:
- Canonicalize URLs
- Remove fragments (#)
- Remove tracking query parameters
- Normalize scheme, netloc, path
- Prevent crawler explosion due to URL variants
"""

from urllib.parse import (
    urlparse,
    urlunparse,
    parse_qsl,
    urlencode,
)

# Common tracking / junk query parameters
TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "yclid",
    "_ga",
    "_gid",
    "ref",
    "referrer",
    "source",
}


def normalize_url(url: str | None) -> str | None:
    """
    Normalize URL into a canonical, crawl-safe form.

    Returns:
        Normalized URL string, or None if invalid / unsupported
    """

    if not url or not isinstance(url, str):
        return None

    url = url.strip()

    # Reject non-http URLs early
    if url.startswith(("mailto:", "javascript:", "tel:", "data:")):
        return None

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    if parsed.scheme not in ("http", "https"):
        return None

    # Normalize scheme & netloc
    scheme = "https"  # enforce https canonical form
    netloc = parsed.netloc.lower()

    # Remove www. only if site serves same content (safe default: keep it)
    # netloc = netloc.lstrip("www.")

    # Normalize path
    path = parsed.path or "/"

    # Remove trailing slash except root
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    # Clean query params
    query_params = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=False)
        if k.lower() not in TRACKING_PARAMS
    ]

    query = urlencode(query_params, doseq=True)

    # Drop fragments completely
    fragment = ""

    normalized = urlunparse(
        (scheme, netloc, path, "", query, fragment)
    )

    return normalized
