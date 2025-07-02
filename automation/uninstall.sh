#!/bin/bash

echo "Uninstalling CAS Alert Scraper..."

# Unload the launch agent
PLIST_FILE=~/Library/LaunchAgents/com.cas-alert.scraper.plist
if [ -f "$PLIST_FILE" ]; then
    echo "Unloading launch agent..."
    launchctl unload "$PLIST_FILE" 2>/dev/null || true

    # Remove plist file
    echo "Removing plist file..."
    rm -f "$PLIST_FILE"
else
    echo "Launch agent plist file not found. Skipping unload step."
fi

# Optionally remove logs
read -p "Do you want to remove log files? (y/n): " remove_logs
if [[ "$remove_logs" =~ ^[Yy]$ ]]; then
    echo "Removing log files..."
    rm -f ~/Library/Logs/cas-alert-scraper*.log
fi

# Optionally remove credentials from Keychain
read -p "Do you want to remove Google Sheets credentials from Keychain? (y/n): " remove_creds
if [[ "$remove_creds" =~ ^[Yy]$ ]]; then
    echo "Removing credentials from Keychain..."
    # Run Python script to delete credentials
    python3 -c "
import sys
try:
    import keyring
    keyring.delete_password('cas-alert-google-sheets', 'service_account')
    print('Credentials successfully removed from Keychain.')
except Exception as e:
    print(f'Error removing credentials: {e}')
    sys.exit(1)
"
fi

echo "Uninstallation complete!"
echo "Note: Python packages and project files have not been removed."
echo "You can manually remove the project directory if needed."
