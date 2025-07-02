"""
Scraper for the CAS MHRA website
"""
import time
import requests
from typing import List, Optional
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from loguru import logger
from urllib.parse import urljoin

from .base import BaseScraper
from ..config import settings
from ..data.models import Alert


class CASMHRAScraper(BaseScraper):
    """Scraper for the CAS MHRA website"""

    def __init__(self):
        super().__init__()
        self.base_url = settings.CAS_MHRA_URL
        self.viewstate = None
        self.eventvalidation = None

    def get_initial_page(self) -> Optional[BeautifulSoup]:
        """Fetch the initial page and extract ViewState and EventValidation"""
        logger.info(f"Fetching initial page: {self.base_url}")
        response = self.handle_request_errors(self.base_url)
        if not response:
            return None

        self.save_raw_data(response.text, "cas_mhra_initial")
        soup = self.get_soup(response)

        viewstate_element = soup.select_one('input[name="__VIEWSTATE"]')
        eventvalidation_element = soup.select_one('input[name="__EVENTVALIDATION"]')

        if viewstate_element and 'value' in viewstate_element.attrs:
            self.viewstate = viewstate_element['value']
            logger.debug(f"Extracted ViewState: {self.viewstate[:50]}...")
        else:
            logger.warning("ViewState element not found or has no value.")
            self.viewstate = None # Ensure it's None if not found

        if eventvalidation_element and 'value' in eventvalidation_element.attrs:
            self.eventvalidation = eventvalidation_element['value']
            logger.debug(f"Extracted EventValidation: {self.eventvalidation[:50]}...")
        else:
            logger.warning("EventValidation element not found or has no value.")
            self.eventvalidation = None # Ensure it's None if not found

        if self.viewstate is None or self.eventvalidation is None:
             logger.error("Failed to extract required ViewState or EventValidation.")
             return None # Return None if critical elements are missing

        return soup

    def get_page_with_postback(self, eventtarget: str = '', eventargument: str = '') -> Optional[BeautifulSoup]:
        """Fetch a page using ASP.NET postback"""
        if not self.viewstate or not self.eventvalidation:
            logger.error("ViewState or EventValidation not available for postback.")
            return None

        payload = {
            '__EVENTTARGET': eventtarget,
            '__EVENTARGUMENT': eventargument,
            '__VIEWSTATE': self.viewstate,
            '__EVENTVALIDATION': self.eventvalidation,
            # Add other form fields if necessary for search/filtering
        }

        logger.info(f"Fetching page with postback: EVENTTARGET={eventtarget}, EVENTARGUMENT={eventargument}")

        try:
            response = self.session.post(
                self.base_url,
                data=payload,
                timeout=settings.TIMEOUT_SECONDS
            )
            response.raise_for_status()
            self.save_raw_data(response.text, f"cas_mhra_page_{eventargument or 'initial'}")
            soup = self.get_soup(response)

            # Update ViewState and EventValidation for subsequent requests
            new_viewstate = soup.select_one('input[name="__VIEWSTATE"]')
            new_eventvalidation = soup.select_one('input[name="__EVENTVALIDATION"]')

            if new_viewstate:
                self.viewstate = new_viewstate['value']
            if new_eventvalidation:
                self.eventvalidation = new_eventvalidation['value']

            return soup

        except requests.exceptions.RequestException as e:
            logger.error(f"Postback request failed for {self.base_url}: {e}")
            return None

    def parse_alert_table(self, soup: BeautifulSoup) -> List[Alert]:
        """Parse the alert table from the BeautifulSoup object"""
        alerts: List[Alert] = []

        table = soup.select_one('#ctl00_ContentPlaceHolder1_AlertSearchResults1_gvwAlertList')
        if not table:
            logger.warning("Alert table not found on the page.")
            return alerts

        rows = table.select('tr')[1:] # Skip header row

        for row in rows:
            cols = row.select('td')
            if len(cols) >= 5: # Ensure enough columns exist
                try:
                    reference = self.clean_text(cols[0].get_text())
                    title_element = cols[1].select_one('a')
                    title = self.clean_text(title_element.get_text()) if title_element else self.clean_text(cols[1].get_text())
                    alert_url = self.extract_alert_url(self.base_url, str(title_element['href'])) if title_element and 'href' in title_element.attrs else ''
                    originator = self.clean_text(cols[2].get_text())
                    issue_date_str = self.clean_text(cols[3].get_text())
                    status = self.clean_text(cols[4].get_text())

                    # Only attempt to parse if the date string matches the expected format (e.g., 26-Jun-2025)
                    import re
                    date_pattern = re.compile(r'^\d{2}-[A-Za-z]{3}-\d{4}$')
                    if date_pattern.match(issue_date_str):
                        issue_date = self.parse_date(issue_date_str, ['%d-%b-%Y']) # e.g., 26-Jun-2025
                    else:
                        issue_date = None

                    # Determine alert_type based on originator or other cues if available
                    alert_type = "Unknown"
                    if "National Patient Safety Alert" in originator:
                        alert_type = "National Patient Safety Alert"
                    elif "CMO Messaging" in originator:
                         alert_type = "CMO Messaging"
                    # Add more logic here to categorize other types if needed

                    if issue_date: # Only add if date is valid
                        alert = Alert(
                            reference=reference,
                            title=title,
                            originator=originator,
                            issue_date=issue_date,
                            status=status,
                            alert_type=alert_type,
                            source='CAS',
                            url=alert_url
                        )
                        # Enrich alert with detail page data
                        if alert_url:
                            self.enrich_alert_with_detail(alert)
                        alerts.append(alert)
                        logger.debug(f"Parsed alert: {alert.reference} - {alert.title}")
                    else:
                        logger.warning(f"Skipping alert due to invalid date: {reference} - {title} | Raw date: {issue_date_str} | Row: {[col.get_text() for col in cols]}")

                except Exception as e:
                    logger.error(f"Error parsing row: {row.get_text()}. Error: {e}")
                    continue

        return alerts

    def enrich_alert_with_detail(self, alert: Alert):
        """Fetch and parse the alert detail page, updating the Alert object with extra fields."""
        try:
            response = self.session.get(alert.url, timeout=settings.TIMEOUT_SECONDS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Example selectors - these may need adjustment based on actual page structure
            def get_text_by_label(label):
                el = soup.find(string=lambda t: isinstance(t, str) and label in t)
                if el and hasattr(el, "parent") and isinstance(el.parent, Tag):
                    sibling = el.parent.find_next_sibling()
                    if sibling and isinstance(sibling, Tag):
                        return sibling.get_text(strip=True)
                return ""

            # Originator (may already be set, but can be updated if more detail is present)
            originator = get_text_by_label("Originator:")
            if originator:
                alert.originator = originator

            # Action category
            alert.action_category = get_text_by_label("Action category:")

            # Broadcast content
            broadcast_content = ""
            bc_label = soup.find(string=lambda t: isinstance(t, str) and "Broadcast content:" in t)
            if bc_label and hasattr(bc_label, "parent") and isinstance(bc_label.parent, Tag):
                bc_parent = bc_label.parent
                bc_content = bc_parent.find_next_sibling()
                if bc_content and isinstance(bc_content, Tag):
                    broadcast_content = bc_content.get_text(separator="\n", strip=True)
            alert.broadcast_content = broadcast_content

            # Additional information
            alert.additional_info = get_text_by_label("Additional information:")

            # Action deadlines
            alert.action_underway_deadline = get_text_by_label("Action underway deadline:")
            alert.action_complete_deadline = get_text_by_label("Action complete deadline:")

            # Attachments (collect all links in the attachments section)
            attachments = []
            attach_label = soup.find(string=lambda t: isinstance(t, str) and "Attachments:" in t)
            if attach_label and hasattr(attach_label, "parent") and isinstance(attach_label.parent, Tag):
                attach_parent = attach_label.parent
                attach_section = attach_parent.find_next_sibling()
                if attach_section and isinstance(attach_section, Tag):
                    for a in attach_section.find_all("a", href=True):
                        if isinstance(a, Tag):
                            href = a.get("href", "")
                            text = a.get_text(strip=True)
                            attachments.append(f"{text} ({href})")
            alert.attachments = ", ".join(attachments) if attachments else ""

        except Exception as e:
            logger.warning(f"Failed to enrich alert from detail page {alert.url}: {e}")

    def scrape(self) -> List[Alert]:
        """Scrape all pages of alerts from the CAS MHRA website"""
        self.alerts = []

        # Get initial page
        soup = self.get_initial_page()
        if not soup:
            logger.error("Failed to fetch initial CAS MHRA page.")
            return []

        self.alerts.extend(self.parse_alert_table(soup))
        self.rate_limit()

        # Find pagination links
        pagination_links = soup.select('#ctl00_ContentPlaceHolder1_AlertSearchResults1_gvwAlertList a')

        # Extract page numbers from links and sort them
        page_numbers = []
        current_page_span = soup.select_one('.gridview_pager span')
        current_page_text = current_page_span.get_text() if current_page_span else None

        for link in pagination_links:
            try:
                link_text = link.get_text()
                # Check if the link text is a digit and not the current page
                if link_text.isdigit() and link_text != current_page_text:
                     page_numbers.append(int(link_text))
            except ValueError:
                continue # Ignore non-numeric links (like '...')

        page_numbers = sorted(list(set(page_numbers))) # Get unique sorted page numbers

        logger.info(f"Found pagination links for pages: {page_numbers}")

        # Iterate through subsequent pages using postback
        for page_num in page_numbers:
            # The eventtarget for pagination links is typically the GridView ID
            # The eventargument is the page number
            eventtarget = 'ctl00$ContentPlaceHolder1$AlertSearchResults1$gvwAlertList'
            eventargument = f'Page${page_num}'

            soup = self.get_page_with_postback(eventtarget=eventtarget, eventargument=eventargument)
            if soup:
                self.alerts.extend(self.parse_alert_table(soup))
                self.rate_limit()
            else:
                logger.warning(f"Failed to fetch page {page_num}. Stopping pagination.")
                break # Stop if a page fails to load

        logger.info(f"Finished scraping CAS MHRA. Total alerts found: {len(self.alerts)}")
        return self.alerts
