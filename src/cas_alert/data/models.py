"""
Data models for CAS Alert Scraper
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import hashlib


@dataclass
class Alert:
    """Data model for a CAS alert"""
    reference: str
    title: str
    originator: str
    issue_date: datetime
    status: str
    alert_type: str
    source: str  # 'CAS' or 'GOVUK'
    url: str
    medical_specialty: Optional[str] = None
    scraped_at: Optional[datetime] = None
    hash_id: Optional[str] = None

    # New fields for detail page scraping
    action_category: Optional[str] = None
    broadcast_content: Optional[str] = None
    additional_info: Optional[str] = None
    action_underway_deadline: Optional[str] = None
    action_complete_deadline: Optional[str] = None
    attachments: Optional[str] = None  # Comma-separated list of attachment names/URLs

    def __post_init__(self):
        """Generate hash ID and set scraped_at if not provided"""
        if self.scraped_at is None:
            self.scraped_at = datetime.now()

        if self.hash_id is None:
            self.hash_id = self.generate_hash()

    def generate_hash(self) -> str:
        """Generate a unique hash for duplicate detection"""
        content = f"{self.reference}|{self.title}|{self.originator}|{self.issue_date.strftime('%Y-%m-%d')}"
        return hashlib.md5(content.encode()).hexdigest()

    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame/Google Sheets"""
        return {
            'Reference': self.reference,
            'Title': self.title,
            'Originator': self.originator,
            'Issue Date': self.issue_date.strftime('%Y-%m-%d'),
            'Status': self.status,
            'Alert Type': self.alert_type,
            'Source': self.source,
            'URL': self.url,
            'Medical Specialty': self.medical_specialty or '',
            'Scraped At': self.scraped_at.strftime('%Y-%m-%d %H:%M:%S') if self.scraped_at else '',
            'Hash ID': self.hash_id,
            'Action Category': self.action_category or '',
            'Broadcast Content': self.broadcast_content or '',
            'Additional Info': self.additional_info or '',
            'Action Underway Deadline': self.action_underway_deadline or '',
            'Action Complete Deadline': self.action_complete_deadline or '',
            'Attachments': self.attachments or ''
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Alert':
        """Create Alert from dictionary"""
        issue_date_str = data.get('Issue Date', '')
        scraped_at_str = data.get('Scraped At', '')

        return cls(
            reference=data.get('Reference', ''),
            title=data.get('Title', ''),
            originator=data.get('Originator', ''),
            issue_date=datetime.strptime(issue_date_str, '%Y-%m-%d') if issue_date_str else datetime.now(),
            status=data.get('Status', ''),
            alert_type=data.get('Alert Type', ''),
            source=data.get('Source', ''),
            url=data.get('URL', ''),
            medical_specialty=data.get('Medical Specialty') or None,
            scraped_at=datetime.strptime(scraped_at_str, '%Y-%m-%d %H:%M:%S') if scraped_at_str else datetime.now(),
            hash_id=data.get('Hash ID'),
            action_category=data.get('Action Category') or None,
            broadcast_content=data.get('Broadcast Content') or None,
            additional_info=data.get('Additional Info') or None,
            action_underway_deadline=data.get('Action Underway Deadline') or None,
            action_complete_deadline=data.get('Action Complete Deadline') or None,
            attachments=data.get('Attachments') or None
        )
