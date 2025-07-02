#!/bin/bash
set -e

echo "Installing CAS Alert Scraper..."

# Install Python dependencies
echo "Installing Python dependencies from requirements.txt..."
pip3 install -r requirements.txt

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p ~/Library/Logs
mkdir -p ~/Library/LaunchAgents
mkdir -p ~/.config/cas-alert
mkdir -p data/backups
mkdir -p logs

# Copy plist file
echo "Copying plist file to LaunchAgents..."
cp automation/com.cas-alert.scraper.plist ~/Library/LaunchAgents/

# Update paths in plist
echo "Updating paths in plist file..."
PLIST_FILE=~/Library/LaunchAgents/com.cas-alert.scraper.plist
CURRENT_DIR="$(pwd)"
USERNAME="$(whoami)"

# Use sed -i '' for macOS compatibility
sed -i '' "s|/path/to/project|${CURRENT_DIR}|g" "$PLIST_FILE"
sed -i '' "s|username|${USERNAME}|g" "$PLIST_FILE"

# Load the launch agent
echo "Loading the launch agent..."
launchctl load "$PLIST_FILE"

echo "Installation complete!"
echo "The scraper is configured to run daily at 9:00 AM."
echo "Logs will be available at ~/Library/Logs/cas-alert-scraper.log"
echo "You can check its status with: launchctl list | grep cas-alert"
echo "You can run it manually for testing with: launchctl start com.cas-alert.scraper"
