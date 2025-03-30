# Weather Data Processing

Fetches weather data from the SMHI (Swedish Meteorological and Hydrological Institute) API.

```bash
# Setup and run
cd python-scripts
make setup
source venv/bin/activate
python get_data.py

# Cleanup
make clean
```

Required environment variable:
- `SMHI_API_URL`: Base URL for the SMHI API (default: https://opendata-download-metobs.smhi.se/api)