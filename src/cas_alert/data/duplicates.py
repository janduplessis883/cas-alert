"""
Duplicate detection and handling for CAS Alert Scraper
"""
from typing import List
from loguru import logger
from fuzzywuzzy import fuzz

from .models import Alert
from ..config import settings


class DuplicateManager:
    """Handles duplicate detection and merging of alerts"""

    def __init__(self):
        self.duplicate_threshold = settings.DUPLICATE_THRESHOLD

    def is_duplicate(self, alert1: Alert, alert2: Alert) -> bool:
        """
        Check if two alerts are duplicates.
        Uses reference, title similarity, and date.
        """
        # 1. Exact match on reference (most reliable)
        if alert1.reference and alert1.reference == alert2.reference:
            logger.debug(f"Exact reference match: {alert1.reference}")
            return True

        # 2. High similarity on title and same source
        if alert1.source == alert2.source:
            title_similarity = fuzz.ratio(alert1.title.lower(), alert2.title.lower())
            if title_similarity >= self.duplicate_threshold * 100: # fuzz.ratio returns 0-100
                 # Check if dates are close (e.g., within a few days)
                 # For simplicity, let's check if dates are the same for now
                 if alert1.issue_date.date() == alert2.issue_date.date():
                    logger.debug(f"High title similarity ({title_similarity}%) and same date/source: '{alert1.title}' vs '{alert2.title}'")
                    return True

        # 3. Exact match on hash_id (covers reference, title, originator, date)
        if alert1.hash_id and alert1.hash_id == alert2.hash_id:
             logger.debug(f"Exact hash ID match: {alert1.hash_id}")
             return True

        return False

    def remove_duplicates(self, alerts: List[Alert]) -> List[Alert]:
        """
        Remove duplicate alerts from a list.
        Keeps the first occurrence of a duplicate.
        """
        unique_alerts: List[Alert] = []
        seen_hashes = set()
        seen_references = set()

        # Prioritize exact reference matches and hash IDs
        for alert in alerts:
            if alert.reference and alert.reference in seen_references:
                logger.debug(f"Skipping duplicate by reference: {alert.reference}")
                continue
            if alert.hash_id and alert.hash_id in seen_hashes:
                 logger.debug(f"Skipping duplicate by hash ID: {alert.hash_id}")
                 continue

            unique_alerts.append(alert)
            if alert.reference:
                seen_references.add(alert.reference)
            if alert.hash_id:
                 seen_hashes.add(alert.hash_id)

        # Now check for duplicates based on title similarity among the remaining unique alerts
        # This is an O(N^2) operation, so might be slow for very large datasets
        # A more efficient approach for very large datasets would involve clustering or indexing
        # For this scale, pairwise comparison should be acceptable.

        final_unique_alerts: List[Alert] = []
        processed_indices = set()

        for i in range(len(unique_alerts)):
            if i in processed_indices:
                continue

            current_alert = unique_alerts[i]
            is_duplicate_found = False

            for j in range(i + 1, len(unique_alerts)):
                if j in processed_indices:
                    continue

                other_alert = unique_alerts[j]

                if self.is_duplicate(current_alert, other_alert):
                    logger.debug(f"Identified potential duplicate: '{current_alert.title}' vs '{other_alert.title}'")
                    # Decide which one to keep - maybe the one with more complete data or newer scraped_at?
                    # For now, we'll just mark the second one as processed and keep the first.
                    processed_indices.add(j)
                    is_duplicate_found = True # Mark that a duplicate of current_alert was found

            final_unique_alerts.append(current_alert)


        logger.info(f"Removed {len(alerts) - len(final_unique_alerts)} duplicates.")
        return final_unique_alerts

    def merge_duplicates(self, alert1: Alert, alert2: Alert) -> Alert:
        """
        Merge two duplicate alerts into a single, more complete alert.
        (Optional - can be implemented if needed to combine data from different sources)
        For now, we'll just keep the first one found.
        """
        logger.debug(f"Merging duplicates (keeping first): {alert1.reference or alert1.title}")
        return alert1 # Simple merge: keep the first one
