"""
macOS native logging for CAS Alert Scraper
"""
import syslog
from loguru import logger

from ..config import settings


class MacOSLogger:
    """Logs messages to macOS system console using syslog"""

    def __init__(self):
        if settings.LOG_TO_CONSOLE:
            try:
                syslog.openlog("cas-alert-scraper", syslog.LOG_PID, syslog.LOG_DAEMON)
                logger.info("Opened syslog connection for macOS console logging.")
            except Exception as e:
                logger.error(f"Failed to open syslog connection: {e}")
                # Fallback to standard loguru logging if syslog fails
                settings.LOG_TO_CONSOLE = False
                logger.warning("Falling back to standard loguru logging.")
        else:
             logger.info("macOS console logging is disabled in settings.")


    def log_info(self, message: str):
        """Log an informational message"""
        if settings.LOG_TO_CONSOLE:
            try:
                syslog.syslog(syslog.LOG_INFO, message)
            except Exception as e:
                logger.error(f"Failed to write info log to syslog: {e}")
        logger.info(message) # Always log with loguru as well

    def log_warning(self, message: str):
        """Log a warning message"""
        if settings.LOG_TO_CONSOLE:
            try:
                syslog.syslog(syslog.LOG_WARNING, message)
            except Exception as e:
                logger.error(f"Failed to write warning log to syslog: {e}")
        logger.warning(message) # Always log with loguru as well

    def log_error(self, message: str):
        """Log an error message"""
        if settings.LOG_TO_CONSOLE:
            try:
                syslog.syslog(syslog.LOG_ERR, message)
            except Exception as e:
                logger.error(f"Failed to write error log to syslog: {e}")
        logger.error(message) # Always log with loguru as well

    def __del__(self):
        """Close syslog connection on object deletion"""
        if settings.LOG_TO_CONSOLE:
            try:
                syslog.closelog()
                logger.info("Closed syslog connection.")
            except Exception as e:
                logger.error(f"Failed to close syslog connection: {e}")
