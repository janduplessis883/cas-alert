"""
Configuration settings for CAS Alert Scraper
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Google Sheets Configuration
GOOGLE_SHEETS_CREDENTIALS_PATH = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH', 'secrests/google_sheets_secret.json')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID', '1Jyg7rbPwtGIYG039j0CkNh7gA5lf6JRv-Iwaavx-vR0')
GOOGLE_WORKSHEET_NAME = os.getenv('GOOGLE_WORKSHEET_NAME', 'CAS_Alerts')

# Scraping Configuration
SCRAPE_DELAY_SECONDS = int(os.getenv('SCRAPE_DELAY_SECONDS', '2'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
TIMEOUT_SECONDS = int(os.getenv('TIMEOUT_SECONDS', '30'))
USER_AGENT = os.getenv('USER_AGENT', 'CAS-Alert-Scraper/1.0')

# Data Processing
DUPLICATE_THRESHOLD = float(os.getenv('DUPLICATE_THRESHOLD', '0.85'))
MAX_ALERTS_PER_RUN = int(os.getenv('MAX_ALERTS_PER_RUN', '1000'))
BACKUP_ENABLED = os.getenv('BACKUP_ENABLED', 'true').lower() == 'true'

# macOS Specific
ENABLE_NOTIFICATIONS = os.getenv('ENABLE_NOTIFICATIONS', 'true').lower() == 'true'
LOG_TO_CONSOLE = os.getenv('LOG_TO_CONSOLE', 'true').lower() == 'true'
NOTIFICATION_SOUND = os.getenv('NOTIFICATION_SOUND', 'true').lower() == 'true'

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_RETENTION_DAYS = int(os.getenv('LOG_RETENTION_DAYS', '30'))

# URLs
CAS_MHRA_URL = 'https://www.cas.mhra.gov.uk/SearchAlerts.aspx'
GOVUK_ALERTS_URL = 'https://www.gov.uk/drug-device-alerts'

# File paths
DATA_DIR = PROJECT_ROOT / 'data'
BACKUP_DIR = DATA_DIR / 'backups'
LOGS_DIR = PROJECT_ROOT / 'logs'

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
