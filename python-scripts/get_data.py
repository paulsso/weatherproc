import os
import requests
import json
import time
from typing import List, Dict
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Parameter translations from Swedish to English
PARAMETER_TRANSLATIONS = {
    "1": {"name": "Air Temperature", "summary": "Instantaneous value, once per hour"},
    "2": {"name": "Air Temperature", "summary": "Daily average, once per day at 00:00"},
    "3": {"name": "Wind Direction", "summary": "10-minute average, once per hour"},
    "4": {"name": "Wind Speed", "summary": "10-minute average, once per hour"},
    "5": {"name": "Precipitation Amount", "summary": "Daily sum, once per day at 06:00"},
    "6": {"name": "Relative Humidity", "summary": "Instantaneous value, once per hour"},
    "7": {"name": "Precipitation Amount", "summary": "Hourly sum, once per hour"},
    "8": {"name": "Snow Depth", "summary": "Instantaneous value, once per day at 06:00"},
    "9": {"name": "Air Pressure at Sea Level", "summary": "Instantaneous value, once per hour"},
    "10": {"name": "Sunshine Duration", "summary": "Hourly sum, once per hour"},
    "11": {"name": "Global Radiation", "summary": "Hourly average, every hour"},
    "12": {"name": "Visibility", "summary": "Instantaneous value, once per hour"},
    "13": {"name": "Present Weather", "summary": "Instantaneous value, once per hour or 8 times per day"},
    "14": {"name": "Precipitation Amount", "summary": "15-minute sum, 4 times per hour"},
    "15": {"name": "Precipitation Intensity", "summary": "Maximum over 15 minutes, 4 times per hour"},
    "16": {"name": "Total Cloud Cover", "summary": "Instantaneous value, once per hour"},
    "17": {"name": "Precipitation", "summary": "Twice per day at 06:00 and 18:00"},
    "18": {"name": "Precipitation", "summary": "Once per day at 18:00"},
    "19": {"name": "Air Temperature", "summary": "Daily minimum, once per day"},
    "20": {"name": "Air Temperature", "summary": "Daily maximum, once per day"},
    "21": {"name": "Wind Gust", "summary": "Maximum, once per hour"},
    "22": {"name": "Air Temperature", "summary": "Monthly average, once per month"},
    "23": {"name": "Precipitation Amount", "summary": "Monthly sum, once per month"},
    "24": {"name": "Long-wave Radiation", "summary": "Hourly average, every hour"},
    "25": {"name": "Maximum Wind Speed", "summary": "Maximum of 10-minute average over 3 hours, once per hour"},
    "26": {"name": "Air Temperature", "summary": "Minimum, twice per day at 06:00 and 18:00"},
    "27": {"name": "Air Temperature", "summary": "Maximum, twice per day at 06:00 and 18:00"},
    "28": {"name": "Cloud Base", "summary": "Lowest cloud layer, instantaneous value, once per hour"},
    "29": {"name": "Cloud Amount", "summary": "Lowest cloud layer, instantaneous value, once per hour"},
    "30": {"name": "Cloud Base", "summary": "Second cloud layer, instantaneous value, once per hour"},
    "31": {"name": "Cloud Amount", "summary": "Second cloud layer, instantaneous value, once per hour"},
    "32": {"name": "Cloud Base", "summary": "Third cloud layer, instantaneous value, once per hour"},
    "33": {"name": "Cloud Amount", "summary": "Third cloud layer, instantaneous value, once per hour"},
    "34": {"name": "Cloud Base", "summary": "Fourth cloud layer, instantaneous value, once per hour"},
    "35": {"name": "Cloud Amount", "summary": "Fourth cloud layer, instantaneous value, once per hour"},
    "36": {"name": "Cloud Base", "summary": "Lowest cloud base, instantaneous value, once per hour"},
    "37": {"name": "Cloud Base", "summary": "Lowest cloud base, minimum over 15 minutes, once per hour"},
    "38": {"name": "Precipitation Intensity", "summary": "Maximum of 15-minute average, 4 times per hour"},
    "39": {"name": "Dew Point Temperature", "summary": "Instantaneous value, once per hour"},
    "40": {"name": "Ground State", "summary": "Instantaneous value, once per day at 06:00"}
}

def get_weather_data(parameter: str) -> dict:
    METOBS_URL = os.getenv("METOBS_URL")
    station_set = os.getenv("SMHI_STATION_SET")
    period = os.getenv("SMHI_PERIOD")
    
    endpoint = f"/parameter/{parameter}/station-set/{station_set}/period/{period}/data.json"
    headers = {
        "User-Agent": "curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.15.3 zlib/1.2.3 libidn/1.18 libssh2/1.4.2",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate"
    }
    
    response = requests.get(f"{METOBS_URL}{endpoint}", headers=headers)
    assert response.status_code == 200, f"Expected status code 200 for parameter {parameter}, got {response.status_code}"
    
    data = response.json()
    assert isinstance(data, dict)
    return data

def translate_parameter(parameter_info: dict) -> dict:
    key = parameter_info.get("key")
    if key in PARAMETER_TRANSLATIONS:
        translation = PARAMETER_TRANSLATIONS[key]
        return {
            **parameter_info,
            "name": translation["name"],
            "summary": translation["summary"]
        }
    return parameter_info

def translate_period(period_info: dict) -> dict:
    return {
        **period_info,
        "summary": "Data from the last hour"
    }

def save_station_data(data: dict, parameter: str) -> None:
    stations = data.get("station", [])
    parameter_info = translate_parameter(data.get("parameter", {}))
    period_info = translate_period(data.get("period", {}))
    
    for station in stations:
        name = station.get("name", "").replace(" ", "_")
        key = station.get("key", "")
        if name and key:
            # Create station data structure
            station_data = {
                "key": station.get("key"),
                "name": station.get("name"),
                "owner": station.get("owner"),
                "ownerCategory": station.get("ownerCategory"),
                "measuringStations": station.get("measuringStations"),
                "from": station.get("from"),
                "to": station.get("to"),
                "height": station.get("height"),
                "latitude": station.get("latitude"),
                "longitude": station.get("longitude"),
                "parameters": [{
                    "key": parameter_info.get("key"),
                    "name": parameter_info.get("name"),
                    "summary": parameter_info.get("summary"),
                    "unit": parameter_info.get("unit"),
                    "periods": [{
                        "key": period_info.get("key"),
                        "from": period_info.get("from"),
                        "to": period_info.get("to"),
                        "summary": period_info.get("summary"),
                        "sampling": period_info.get("sampling"),
                        "values": station.get("value", [])
                    }]
                }],
                "updated_at": datetime.utcnow()
            }
            
            filename = f"{name}_{key}_{parameter}.json"
            with open(filename, "w") as f:
                json.dump(station_data, f, indent=2)

def fetch_all_parameters() -> None:
    for parameter in range(1, 41):
        try:
            print(f"Fetching data for parameter {parameter}...")
            data = get_weather_data(str(parameter))
            save_station_data(data, str(parameter))
            time.sleep(1)  # Rate limiting to avoid overwhelming the API
        except Exception as e:
            print(f"Error fetching parameter {parameter}: {str(e)}")
            continue

if __name__ == "__main__":
    fetch_all_parameters()