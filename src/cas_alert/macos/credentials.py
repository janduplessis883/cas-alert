"""
macOS Keychain integration for secure credential storage
"""
import keyring
from loguru import logger
import json
from typing import Optional


class MacOSCredentialManager:
    """Manages secure storage of credentials using macOS Keychain"""

    SERVICE_NAME = "cas-alert-google-sheets"
    USERNAME = "service_account"

    @staticmethod
    def store_google_credentials(credentials_json: dict):
        """Store Google Sheets service account credentials in Keychain"""
        try:
            json_string = json.dumps(credentials_json)
            keyring.set_password(MacOSCredentialManager.SERVICE_NAME, MacOSCredentialManager.USERNAME, json_string)
            logger.success("Google Sheets credentials successfully stored in macOS Keychain.")
        except Exception as e:
            logger.error(f"Failed to store Google Sheets credentials in Keychain: {e}")
            raise

    @staticmethod
    def get_google_credentials() -> Optional[dict]:
        """Retrieve Google Sheets service account credentials from Keychain"""
        try:
            json_string = keyring.get_password(MacOSCredentialManager.SERVICE_NAME, MacOSCredentialManager.USERNAME)
            if json_string:
                logger.info("Google Sheets credentials successfully retrieved from macOS Keychain.")
                return json.loads(json_string)
            else:
                logger.warning("Google Sheets credentials not found in macOS Keychain.")
                return None
        except Exception as e:
            logger.error(f"Failed to retrieve Google Sheets credentials from Keychain: {e}")
            return None

    @staticmethod
    def delete_google_credentials():
        """Delete Google Sheets service account credentials from Keychain"""
        try:
            keyring.delete_password(MacOSCredentialManager.SERVICE_NAME, MacOSCredentialManager.USERNAME)
            logger.info("Google Sheets credentials successfully deleted from macOS Keychain.")
        except Exception as e:
            # Catching generic Exception as keyring.NoPasswordFoundException is not directly accessible
            # and the primary goal is to ensure the application functions.
            if "NoPasswordFoundException" in str(e):
                logger.warning("Google Sheets credentials not found in Keychain for deletion.")
            else:
                logger.error(f"Failed to delete Google Sheets credentials from Keychain: {e}")
                raise
