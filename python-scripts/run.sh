#!/bin/bash
set -e

# Run get_data.py
echo "Fetching weather data..."
python get_data.py

# Check if get_data.py completed successfully
if [ $? -eq 0 ]; then
    echo "Data fetch completed successfully. Processing data..."
    python stash_data.py
else
    echo "Error: Data fetch failed. Aborting data processing."
    exit 1
fi 