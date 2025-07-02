import gspread
import pandas as pd
from typing import List, Optional
from loguru import logger
from google.oauth2.service_account import Credentials
from google.auth.exceptions import DefaultCredentialsError, TransportError

from cas_alert.config import settings
from cas_alert.data.models import Alert
from cas_alert.data.processor import DataProcessor


class GoogleSheetsManager:
    \"\"\"Handles interaction with Google Sheets\"\"\"

    def __init__(self):
        self.client: Optional[gspread.Client] = None
        self.sheet: Optional[gspread.Spreadsheet] = None
        self.worksheet: Optional[gspread.Worksheet] = None
        self.processor = DataProcessor()  # Use DataProcessor for DataFrame conversion

    def authenticate(self):
        \"\"\"Authenticate with Google Sheets using service account credentials\"\"\"
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
        \"\"\"Open the specified Google Sheet and Worksheet\"\"\"
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
                logger.warning(f"Worksheet '{settings.GOOGLE_WORKSHEET_NAME}' not found.")
                return None

        except gspread.SpreadsheetNotFound:
            logger.error(f"Google Sheet with ID '{settings.GOOGLE_SHEET_ID}' not found.")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while opening sheet/worksheet: {e}")
            raise

    def get_all_data(self):
        \"\"\"Retrieve all data from the Google Sheet\"\"\"
        if not self.worksheet:
            logger.warning("Worksheet not available to retrieve data.")
            return None

        try:
            # Get all records as a list of lists
            data = self.worksheet.get_all_values()
            logger.info(f"Retrieved {len(data)} rows from Google Sheets.")
            return data

        except Exception as e:
            logger.error(f"Error retrieving data from Google Sheets: {e}")
            return None

    def delete_rows(self, row_indices: List[int]):
        \"\"\"Delete rows from the Google Sheet by index\"\"\"
        if not self.worksheet:
            logger.error("Worksheet not available to delete rows.")
            return

        try:
            # Delete rows in reverse order to avoid index shifting
            for row_index in sorted(row_indices, reverse=True):
                self.worksheet.delete_rows(row_index)
                logger.info(f"Deleted row {row_index} from Google Sheets.")
            logger.success(f"Successfully deleted {len(row_indices)} rows from Google Sheets.")

        except Exception as e:
            logger.error(f"Error deleting rows from Google Sheets: {e}")


def delete_duplicates():
    \"\"\"
    Loads the Google Sheet, identifies duplicate rows based on the 'Hash ID' column,
    and deletes them from the sheet.
    \"\"\"
    logger.info("Starting duplicate deletion process...")

    # Initialize GoogleSheetsManager
    sheets_manager = GoogleSheetsManager()

    try:
        # Authenticate and open the sheet
        sheets_manager.authenticate()
        sheets_manager.open_sheet_and_worksheet()

        if not sheets_manager.worksheet:
            logger.error("Could not open the worksheet. Aborting.")
            return

        # Retrieve all data
        data = sheets_manager.get_all_data()
        if not data:
            logger.error("Could not retrieve data from the worksheet. Aborting.")
            return

        # Extract headers and data rows
        headers = data[0]
        data_rows = data[1:]

        # Find the index of the 'Hash ID' column
        hash_id_index = headers.index('Hash ID')

        # Identify duplicate rows based on 'Hash ID'
        hash_ids = {}
        duplicate_row_indices = []
        for i, row in enumerate(data_rows):
            row_index = i + 2  # Add 2 to account for header row and 0-based indexing
            hash_id = row[hash_id_index]

            if hash_id in hash_ids:
                duplicate_row_indices.append(row_index)
                logger.debug(f"Found duplicate Hash ID: {hash_id} at row {row_index}")
            else:
                hash_ids[hash_id] = row_index

        if not duplicate_row_indices:
            logger.info("No duplicate rows found in the Google Sheet.")
            return

        logger.info(f"Found {len(duplicate_row_indices)} duplicate rows. Deleting...")

        # Delete duplicate rows
        sheets_manager.delete_rows(duplicate_row_indices)

        logger.success("Duplicate deletion process completed.")

    except Exception as e:
        logger.error(f"An error occurred during the duplicate deletion process: {e}")


if __name__ == "__main__":
    # Configure logger
    logger.add("logs/delete_duplicates.log", rotation="500 MB", level="INFO")
    delete_duplicates()
