import os
import json
import glob
from typing import Dict, List
from dotenv import load_dotenv
from pymongo import MongoClient
from collections import defaultdict

load_dotenv()

def get_mongodb_client() -> MongoClient:
    mongodb_uri = os.getenv("MONGODB_URI")
    assert mongodb_uri, "MONGODB_URI environment variable is not set"
    return MongoClient(mongodb_uri)

def parse_filename(filename: str) -> tuple[str, str, str]:
    # Example: Abisko_Aut_188790_1.json -> (Abisko_Aut, 188790, 1)
    parts = os.path.basename(filename).replace(".json", "").split("_")
    station_name = "_".join(parts[:-2])
    station_key = parts[-2]
    parameter = parts[-1]
    return station_name, station_key, parameter

def merge_station_data(files: List[str]) -> Dict:
    # Group files by station
    station_files = defaultdict(list)
    for file in files:
        station_name, station_key, _ = parse_filename(file)
        station_files[(station_name, station_key)].append(file)
    
    merged_data = {}
    for (station_name, station_key), files in station_files.items():
        # Initialize station data structure
        station_data = None
        
        # Merge all parameter data for this station
        for file in files:
            with open(file, 'r') as f:
                data = json.load(f)
                
            if station_data is None:
                station_data = {
                    "key": data["key"],
                    "name": data["name"],
                    "owner": data["owner"],
                    "ownerCategory": data["ownerCategory"],
                    "measuringStations": data["measuringStations"],
                    "from": data["from"],
                    "to": data["to"],
                    "height": data["height"],
                    "latitude": data["latitude"],
                    "longitude": data["longitude"],
                    "parameters": [],
                    "updated_at": data["updated_at"]
                }
            
            # Add parameter data
            station_data["parameters"].extend(data["parameters"])
        
        merged_data[(station_name, station_key)] = station_data
    
    return merged_data

def save_to_mongodb(data: Dict, db: MongoClient) -> None:
    collection = db.weather_db.weather_data
    
    for station_data in data.values():
        # Update or insert the document
        collection.update_one(
            {"key": station_data["key"]},
            {"$set": station_data},
            upsert=True
        )

def main() -> None:
    # Get all JSON files in the current directory
    json_files = glob.glob("*.json")
    
    # Filter files that match our naming convention
    station_files = [f for f in json_files if len(f.split("_")) >= 4 and f.endswith(".json")]
    
    if not station_files:
        print("No matching JSON files found")
        return
    
    # Merge data from all files
    print("Merging station data...")
    merged_data = merge_station_data(station_files)
    
    # Connect to MongoDB and save data
    print("Saving to MongoDB...")
    client = get_mongodb_client()
    try:
        save_to_mongodb(merged_data, client)
        print(f"Successfully saved {len(merged_data)} stations to MongoDB")
    finally:
        client.close()

if __name__ == "__main__":
    main() 