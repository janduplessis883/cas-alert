#!/usr/bin/env python3
"""
CAS Alert Scraper - Main Entry Point
Designed for macOS automation via launchd
"""
import sys
from pathlib import Path

# Ensure project root is on sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from cas_alert.macos.logging import MacOSLogger
from cas_alert.macos.notifications import MacOSNotifier
from cas_alert.scrapers.cas_mhra import CASMHRAScraper
from cas_alert.scrapers.govuk import GOVUKScraper
from cas_alert.data.processor import DataProcessor
from cas_alert.storage.google_sheets import GoogleSheetsManager

def main():
    logger = MacOSLogger()
    notifier = MacOSNotifier()

    try:
        logger.log_info("Starting CAS Alert scraping job")

        # Scrape data from both sources
        cas_scraper = CASMHRAScraper()
        govuk_scraper = GOVUKScraper()
        cas_alerts = cas_scraper.scrape()
        govuk_alerts = govuk_scraper.scrape()

        # Process and deduplicate alerts
        processor = DataProcessor()
        all_alerts = processor.process_and_deduplicate(cas_alerts + govuk_alerts)

        # Update Google Sheets
        sheets_manager = GoogleSheetsManager()
        new_count = sheets_manager.update_with_new_alerts(all_alerts)

        # Send notification if new alerts found
        if new_count > 0:
            notifier.send_notification(
                "CAS Alert Scraper",
                f"Found {new_count} new alerts and updated Google Sheets"
            )

        logger.log_info(f"Scraping job completed successfully. {new_count} new alerts found.")

    except Exception as e:
        error_msg = f"Scraping job failed: {e}"
        logger.log_error(error_msg)
        notifier.send_notification("CAS Alert Scraper - Error", error_msg)
        sys.exit(1)

if __name__ == "__main__":
    main()
