"""
Data processing and deduplication for CAS Alert Scraper
"""
from typing import List
from loguru import logger
import pandas as pd

from .models import Alert
from .duplicates import DuplicateManager


class DataProcessor:
    """Handles data processing and deduplication"""

    def __init__(self):
        self.duplicate_manager = DuplicateManager()

    def process_and_deduplicate(self, alerts: List[Alert]) -> List[Alert]:
        """
        Process a list of raw alerts, including deduplication.
        """
        logger.info(f"Starting data processing and deduplication for {len(alerts)} alerts.")

        # Step 1: Basic validation and standardization (if needed)
        # For now, the Alert model handles basic validation and hash generation
        processed_alerts = alerts # Assuming alerts are already in Alert model format

        # Step 2: Deduplication
        unique_alerts = self.duplicate_manager.remove_duplicates(processed_alerts)

        logger.info(f"Finished data processing. Found {len(unique_alerts)} unique alerts.")
        return unique_alerts

    def alerts_to_dataframe(self, alerts: List[Alert]) -> pd.DataFrame:
        """Convert a list of Alert objects to a pandas DataFrame"""
        if not alerts:
            return pd.DataFrame()

        # Convert list of Alert objects to list of dictionaries
        alerts_data = [alert.to_dict() for alert in alerts]

        # Create DataFrame
        df = pd.DataFrame(alerts_data)

        # Ensure columns are in a consistent order
        column_order = [
            'Reference', 'Title', 'Originator', 'Issue Date', 'Status',
            'Alert Type', 'Source', 'URL', 'Medical Specialty', 'Scraped At', 'Hash ID'
        ]
        # Add any columns present in the data but not in the order list
        existing_columns = df.columns.tolist()
        for col in existing_columns:
            if col not in column_order:
                column_order.append(col)

        # Reindex DataFrame to enforce column order
        df = df.reindex(columns=column_order)

        # Convert 'Issue Date' and 'Scraped At' to datetime objects if they aren't already
        # This is useful for sorting and time-based operations later
        try:
            df['Issue Date'] = pd.to_datetime(df['Issue Date'], errors='coerce')
        except Exception as e:
            logger.warning(f"Could not convert 'Issue Date' to datetime: {e}")

        try:
            df['Scraped At'] = pd.to_datetime(df['Scraped At'], errors='coerce')
        except Exception as e:
            logger.warning(f"Could not convert 'Scraped At' to datetime: {e}")


        return df

    def dataframe_to_alerts(self, df: pd.DataFrame) -> List[Alert]:
        """Convert a pandas DataFrame to a list of Alert objects"""
        if df.empty:
            return []

        alerts: List[Alert] = []
        for index, row in df.iterrows():
            try:
                alert = Alert.from_dict(row.to_dict())
                alerts.append(alert)
            except Exception as e:
                logger.error(f"Error converting DataFrame row to Alert object: {row.to_dict()}. Error: {e}")
                continue

        return alerts
