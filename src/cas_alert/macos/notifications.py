"""
macOS native notifications for CAS Alert Scraper
"""
import subprocess
from loguru import logger

from ..config import settings


class MacOSNotifier:
    """Sends native macOS notifications"""

    @staticmethod
    def send_notification(title: str, message: str, sound: bool = settings.NOTIFICATION_SOUND):
        """Send a notification using osascript"""
        if not settings.ENABLE_NOTIFICATIONS:
            logger.info("macOS notifications are disabled in settings.")
            return

        try:
            cmd = [
                'osascript', '-e',
                f'display notification "{message}" with title "{title}"'
            ]
            if sound:
                cmd.extend(['-e', 'beep'])

            logger.debug(f"Executing notification command: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info(f"Sent notification: '{title}' - '{message}'")

        except FileNotFoundError:
            logger.error("osascript command not found. Is this running on macOS?")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error sending notification: {e.stderr}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending notification: {e}")
