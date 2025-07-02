"""
Base scraper class for CAS Alert Scraper
"""
import time
import requests
from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger

from ..config import settings
from ..data.models import Alert


class BaseScraper(ABC):
    """Abstract base class for scrapers"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': settings.USER_AGENT
        })
        self.alerts: List[Alert] = []

    def handle_request_errors(self, url: str, retries: int = 0) -> Optional[requests.Response]:
        """Handle HTTP requests with error handling and retries"""
        try:
            response = self.session.get(
                url,
                timeout=settings.TIMEOUT_SECONDS
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            if retries < settings.MAX_RETRIES:
                logger.info(f"Retrying... ({retries + 1}/{settings.MAX_RETRIES})")
                time.sleep(2 ** retries)  # Exponential backoff
                return self.handle_request_errors(url, retries + 1)
            return None

    def parse_date(self, date_str: str, formats: List[str]) -> Optional[datetime]:
        """Parse date string with multiple format attempts"""
        if not date_str:
            return None

        # Clean the date string
        date_str = date_str.strip()

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        logger.warning(f"Could not parse date: {date_str}")
        return None

    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""

        # Remove extra whitespace and normalize
        text = ' '.join(text.split())
        return text.strip()

    def rate_limit(self):
        """Apply rate limiting between requests"""
        time.sleep(settings.SCRAPE_DELAY_SECONDS)

    def save_raw_data(self, data: str, filename: str):
        """Save raw HTML data for debugging"""
        if settings.BACKUP_ENABLED:
            import re
            # Replace invalid filename characters with underscores
            safe_filename = re.sub(r'[^A-Za-z0-9._-]', '_', filename)
            filepath = settings.BACKUP_DIR / f"{safe_filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(data)
            logger.debug(f"Raw data saved to {filepath}")

    @abstractmethod
    def scrape(self) -> List[Alert]:
        """Abstract method to be implemented by subclasses"""
        pass

    def get_soup(self, response: requests.Response) -> BeautifulSoup:
        """Create BeautifulSoup object from response"""
        return BeautifulSoup(response.content, 'html.parser')

    def extract_alert_url(self, base_url: str, relative_url: str) -> str:
        """Construct full URL from base and relative URLs"""
        if relative_url.startswith('http'):
            return relative_url

        if relative_url.startswith('/'):
            from urllib.parse import urljoin
            return urljoin(base_url, relative_url)

        return f"{base_url.rstrip('/')}/{relative_url.lstrip('/')}"
