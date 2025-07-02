"""
Google Sheets integration for CAS Alert Scraper
"""
import gspread
import pandas as pd
from typing import List, Optional
from loguru import logger
from google.oauth2.service_account import Credentials
from google.auth.exceptions import DefaultCredentialsError, TransportError

from ..config import settings
from ..data.models import Alert
from ..data.processor import DataProcessor


class GoogleSheetsManager:
    """Handles interaction with Google Sheets"""

    def __init__(self):
        self.client: Optional[gspread.Client] = None
        self.sheet: Optional[gspread.Spreadsheet] = None
        self.worksheet: Optional[gspread.Worksheet] = None
        self.processor = DataProcessor() # Use DataProcessor for DataFrame conversion

    def authenticate(self):
        """Authenticate with Google Sheets using service account credentials"""
        # Define the scope
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        # Load credentials from the JSON file
        credentials_path = settings.PROJECT_ROOT / settings.GOOGLE_SHEETS_CREDENTIALS_PATH
        try:
            logger.info(f"Attempting to load Google Sheets credentials from: {credentials_path}")

            creds = Credentials.from_service_account_file(
                credentials_path, scopes=scope
            )

            # Authorize the client
            self.client = gspread.authorize(creds)
            logger.success("Successfully authenticated with Google Sheets.")

        except FileNotFoundError:
            logger.error(f"Google Sheets credentials file not found at {credentials_path}")
            raise
        except DefaultCredentialsError as e:
            logger.error(f"Default Google Cloud credentials not found: {e}")
            raise
        except TransportError as e:
            logger.error(f"Transport error during Google Sheets authentication: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during Google Sheets authentication: {e}")
            raise

    def open_sheet_and_worksheet(self):
        """Open the specified Google Sheet and Worksheet"""
        if not self.client:
            logger.error("Google Sheets client not authenticated.")
            return

        try:
            self.sheet = self.client.open_by_key(settings.GOOGLE_SHEET_ID)
            logger.info(f"Successfully opened Google Sheet: {self.sheet.title}")

            try:
                self.worksheet = self.sheet.worksheet(settings.GOOGLE_WORKSHEET_NAME)
                logger.info(f"Successfully opened Worksheet: {self.worksheet.title}")
            except gspread.WorksheetNotFound:
                logger.warning(f"Worksheet '{settings.GOOGLE_WORKSHEET_NAME}' not found. Creating it.")
                # Create the worksheet if it doesn't exist
                self.worksheet = self.sheet.add_worksheet(
                    title=settings.GOOGLE_WORKSHEET_NAME,
                    rows=100, # Initial rows, can be expanded
                    cols=11 # Number of columns based on Alert model
                )
                logger.success(f"Created new Worksheet: {self.sheet.title}")
                # Add headers to the new worksheet
                headers = [
                    'Reference', 'Title', 'Originator', 'Issue Date', 'Status',
                    'Alert Type', 'Source', 'URL', 'Medical Specialty', 'Scraped At', 'Hash ID'
                ]
                self.worksheet.append_row(headers)
                logger.info("Added headers to the new worksheet.")

        except gspread.SpreadsheetNotFound:
            logger.error(f"Google Sheet with ID '{settings.GOOGLE_SHEET_ID}' not found.")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while opening sheet/worksheet: {e}")
            raise

    def get_existing_alerts(self) -> List[Alert]:
        """Retrieve existing alerts from the Google Sheet"""
        if not self.worksheet:
            logger.warning("Worksheet not available to retrieve data.")
            return []

        try:
            # Get all records as a list of dictionaries
            data = self.worksheet.get_all_records()
            logger.info(f"Retrieved {len(data)} records from Google Sheets.")

            # Convert to DataFrame and then to Alert objects
            df = pd.DataFrame(data)
            existing_alerts = self.processor.dataframe_to_alerts(df)
            logger.info(f"Converted {len(existing_alerts)} records to Alert objects.")
            return existing_alerts

        except Exception as e:
            logger.error(f"Error retrieving data from Google Sheets: {e}")
            return []

    def update_with_new_alerts(self, new_alerts: List[Alert]) -> int:
        """
        Update the Google Sheet with new alerts, avoiding duplicates.
        Returns the number of new alerts added.
        """
        if not self.client:
            self.authenticate()
        if not self.sheet or not self.worksheet:
            self.open_sheet_and_worksheet()

        if not self.worksheet:
            logger.error("Google Sheets worksheet is not available for updating.")
            return 0

        logger.info(f"Attempting to update Google Sheets with {len(new_alerts)} new alerts.")

        # Get existing alerts from the sheet
        existing_alerts = self.get_existing_alerts()
        existing_hashes = {alert.hash_id for alert in existing_alerts if alert.hash_id}
        existing_references = {alert.reference for alert in existing_alerts if alert.reference}

        alerts_to_add: List[Alert] = []
        for alert in new_alerts:
            # Check for duplicates based on hash ID and reference
            if alert.hash_id and alert.hash_id in existing_hashes:
                logger.debug(f"Skipping alert (duplicate hash): {alert.title}")
                continue
            if alert.reference and alert.reference in existing_references:
                 logger.debug(f"Skipping alert (duplicate reference): {alert.reference}")
                 continue

            # Add to list of alerts to add
            alerts_to_add.append(alert)
            # Add to seen sets to avoid adding duplicates within the new_alerts list itself
            if alert.hash_id:
                existing_hashes.add(alert.hash_id)
            if alert.reference:
                 existing_references.add(alert.reference)


        if not alerts_to_add:
            logger.info("No new unique alerts to add to Google Sheets.")
            return 0

        logger.info(f"Adding {len(alerts_to_add)} new unique alerts to Google Sheets.")

        # Convert new alerts to DataFrame and then to list of lists for gspread
        new_alerts_df = self.processor.alerts_to_dataframe(alerts_to_add)
        # Ensure column order matches the sheet headers
        headers = [
            'Reference', 'Title', 'Originator', 'Issue Date', 'Status',
            'Alert Type', 'Source', 'URL', 'Medical Specialty', 'Scraped At', 'Hash ID'
        ]
        new_alerts_df = new_alerts_df.reindex(columns=headers)

        # Convert all datetime-like columns to string to avoid JSON serialization errors
        import pandas as pd
        for col in new_alerts_df.columns:
            if pd.api.types.is_datetime64_any_dtype(new_alerts_df[col]) or new_alerts_df[col].apply(lambda x: hasattr(x, 'isoformat')).any():
                new_alerts_df[col] = new_alerts_df[col].apply(lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x))

        # Convert DataFrame to list of lists (excluding header)
        data_to_append = new_alerts_df.values.tolist()

        try:
            # Append data to the worksheet
            from gspread.utils import ValueInputOption
            self.worksheet.append_rows(data_to_append, value_input_option=ValueInputOption.user_entered)
            logger.success(f"Successfully added {len(alerts_to_add)} new alerts to Google Sheets.")
            return len(alerts_to_add)

        except Exception as e:
            logger.error(f"Error appending data to Google Sheets: {e}")
            return 0

    def format_worksheet(self):
        """Apply basic formatting to the worksheet (optional)"""
        if not self.worksheet:
            logger.warning("Worksheet not available for formatting.")
            return

        try:
            # Auto-resize columns (not supported by gspread, so this is commented out)
            # self.worksheet.auto_resize_columns()
            # logger.debug("Auto-resized columns.")

            # Set header row bold
            self.worksheet.format("A1:K1", {"textFormat": {"bold": True}})
            logger.debug("Formatted header row.")

            # Add filters
            self.worksheet.set_basic_filter()
            logger.debug("Added basic filter.")

            logger.info("Applied basic worksheet formatting.")

        except Exception as e:
            logger.error(f"Error applying formatting to Google Sheets worksheet: {e}")
