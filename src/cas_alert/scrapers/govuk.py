"""
Scraper for the GOV.UK Drug/Device Alerts website
"""
import time
from typing import List, Optional
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from loguru import logger
from urllib.parse import urljoin

from .base import BaseScraper
from ..config import settings
from ..data.models import Alert


class GOVUKScraper(BaseScraper):
    """Scraper for the GOV.UK Drug/Device Alerts website"""

    def __init__(self):
        super().__init__()
        self.base_url = settings.GOVUK_ALERTS_URL

    def get_alert_list_page(self, page_url: str) -> Optional[BeautifulSoup]:
        """Fetch a page from the alert list"""
        logger.info(f"Fetching GOV.UK alert list page: {page_url}")
        response = self.handle_request_errors(page_url)
        if not response:
            return None

        self.save_raw_data(response.text, f"govuk_list_{page_url.split('=')[-1]}")
        return self.get_soup(response)

    def parse_alert_list(self, soup: BeautifulSoup) -> List[Alert]:
        """Parse the list of alerts on a page"""
        alerts: List[Alert] = []

        alert_items = soup.select('.gem-c-document-list__item')

        if not alert_items:
            logger.warning("No alert items found on the GOV.UK page.")
            return alerts

        for item in alert_items:
            try:
                title_element = item.select_one('.gem-c-document-list__item-title a')
                title = self.clean_text(title_element.get_text()) if title_element else ""
                alert_url = self.extract_alert_url(self.base_url, str(title_element['href'])) if title_element and 'href' in title_element.attrs else ''

                metadata_elements = item.select('.gem-c-document-list__item-metadata dd')

                alert_type = self.clean_text(metadata_elements[0].get_text()) if len(metadata_elements) > 0 else "Unknown"
                # Medical specialty is often the second dd, but can be missing
                medical_specialty = self.clean_text(metadata_elements[1].get_text()) if len(metadata_elements) > 1 else None
                issue_date_str = self.clean_text(metadata_elements[-1].get_text()) if len(metadata_elements) > 0 else "" # Issue date is usually the last dd

                issue_date = self.parse_date(issue_date_str, ['%d %B %Y']) # e.g., 30 June 2025

                # GOV.UK alerts don't have a standard 'Reference' or 'Originator' in the list view
                # We might need to scrape individual pages for more details later if needed
                # For now, use URL as reference and source as originator placeholder
                reference = alert_url or title # Use URL or title as reference
                originator = "MHRA/GOV.UK"
                status = "Issued" # Default status for listed alerts

                if title and alert_url and issue_date: # Ensure essential fields are present
                    alert = Alert(
                        reference=reference,
                        title=title,
                        originator=originator,
                        issue_date=issue_date,
                        status=status,
                        alert_type=alert_type,
                        source='GOVUK',
                        url=alert_url,
                        medical_specialty=medical_specialty
                    )
                    # Enrich alert with detail page data
                    if alert_url:
                        self.enrich_alert_with_detail(alert)
                    alerts.append(alert)
                    logger.debug(f"Parsed GOV.UK alert: {alert.title}")
                else:
                    logger.warning(f"Skipping GOV.UK alert due to missing essential data: Title='{title}', URL='{alert_url}', Date='{issue_date_str}'")

            except Exception as e:
                logger.error(f"Error parsing GOV.UK alert item: {item.get_text()}. Error: {e}")
                continue

        return alerts

    def enrich_alert_with_detail(self, alert: Alert):
        """Fetch and parse the alert detail page, updating the Alert object with extra fields."""
        try:
            import re
            response = self.session.get(alert.url, timeout=settings.TIMEOUT_SECONDS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Title (may be more detailed on the page)
            page_title = soup.select_one("h1")
            if page_title:
                alert.title = page_title.get_text(strip=True)

            # Published date
            published = soup.find("time", attrs={"data-module": "govuk-datetime"})
            if published and isinstance(published, Tag) and published.get("datetime"):
                try:
                    dt_val = published.get("datetime")
                    if isinstance(dt_val, str):
                        alert.issue_date = datetime.strptime(dt_val[:10], "%Y-%m-%d")
                except Exception:
                    pass

            # Extract reference number (e.g., DMRC reference number, batch, etc.)
            # Try to find a reference number in the content
            content_text = soup.get_text(separator="\n", strip=True)
            ref_match = re.search(r"(DMRC[-\s:]?\d+)", content_text)
            if ref_match:
                alert.reference = ref_match.group(1)

            # Try to extract batch numbers, background, advice, etc.
            # This is heuristic and may need adjustment for different alert types
            def extract_section(label):
                # Find the section header, then get the next sibling or following text
                header = soup.find(lambda tag: isinstance(tag, Tag) and tag.name in ["h2", "h3"] and label.lower() in tag.get_text(strip=True).lower())
                if header:
                    # Try to get all text until the next header of the same level
                    texts = []
                    for sib in header.find_next_siblings():
                        if isinstance(sib, Tag) and sib.name in ["h2", "h3"]:
                            break
                        if isinstance(sib, Tag):
                            texts.append(sib.get_text(separator="\n", strip=True))
                    return "\n".join(texts).strip()
                return ""

            alert.additional_info = extract_section("Additional information") or extract_section("Additional Information")
            alert.broadcast_content = extract_section("Background")
            alert.action_category = extract_section("Advice for Healthcare Professionals")
            # Attachments (look for download links)
            attachments = []
            for a in soup.find_all("a", href=True):
                if isinstance(a, Tag):
                    href = a.get("href", "")
                    text = a.get_text(strip=True)
                    if "download" in text.lower() or (isinstance(href, str) and href.endswith(".pdf")):
                        attachments.append(f"{text} ({href})")
            alert.attachments = ", ".join(attachments) if attachments else ""

        except Exception as e:
            logger.warning(f"Failed to enrich GOV.UK alert from detail page {alert.url}: {e}")

    def get_next_page_url(self, soup: BeautifulSoup) -> Optional[str]:
        """Find the URL for the next page in pagination"""
        next_link = soup.select_one('.pagination__next a')
        if next_link and 'href' in next_link.attrs:
            next_page_relative_url = str(next_link['href'])
            return self.extract_alert_url(self.base_url, next_page_relative_url)
        return None

    def scrape(self) -> List[Alert]:
        """Scrape all pages of alerts from the GOV.UK website"""
        self.alerts = []
        current_url = self.base_url

        while current_url:
            soup = self.get_alert_list_page(current_url)
            if soup:
                self.alerts.extend(self.parse_alert_list(soup))
                current_url = self.get_next_page_url(soup)
                if current_url:
                    self.rate_limit() # Rate limit before fetching the next page
            else:
                logger.error(f"Failed to fetch GOV.UK page: {current_url}. Stopping pagination.")
                break # Stop if a page fails to load

        logger.info(f"Finished scraping GOV.UK. Total alerts found: {len(self.alerts)}")
        return self.alerts
