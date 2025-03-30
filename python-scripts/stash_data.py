import os
import json
import glob
from typing import Dict, List
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from collections import defaultdict

load_dotenv()

def validate_mongodb_connection(client: MongoClient) -> bool:
    try:
        # Ping the server to check connection
        client.admin.command('ping')
        
        # Try to access the database and collection
        db = client.weather_db
        collection = db.weather_data
        
        # Try a simple operation
        collection.find_one()
        
        print("Successfully connected to MongoDB")
        return True
    except ConnectionFailure:
        print("Failed to connect to MongoDB. Please check if the service is running and accessible.")
        return False
    except OperationFailure as e:
        print(f"Failed to authenticate with MongoDB: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error while validating MongoDB connection: {str(e)}")
        return False

def setup_indexes(collection) -> None:
    # Index for efficient station lookups
    collection.create_index([("key", ASCENDING)], unique=True)
    
    # Index for efficient time-based queries
    collection.create_index([
        ("key", ASCENDING),
        ("parameters.key", ASCENDING),
        ("parameters.periods.from", ASCENDING)
    ])

def get_mongodb_client() -> MongoClient:
    mongodb_uri = os.getenv("MONGODB_URI")
    assert mongodb_uri, "MONGODB_URI environment variable is not set"
    
    client = MongoClient(mongodb_uri)
    if not validate_mongodb_connection(client):
        raise ConnectionError("Failed to establish valid MongoDB connection")
    
    # Setup indexes
    setup_indexes(client.weather_db.weather_data)
    
    return client

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
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                    
                if station_data is None:
                    station_data = {
                        "key": data["key"],
                        "name": data["name"],
                        "owner": data["owner"],
                        "ownerCategory": data["ownerCategory"],
                        "measuringStations": data["measuringStations"],
                        "height": data["height"],
                        "latitude": data["latitude"],
                        "longitude": data["longitude"],
                        "parameters": [],
                        "last_updated": datetime.now(datetime.UTC)
                    }
                
                # Add parameter data
                station_data["parameters"].extend(data["parameters"])
            except json.JSONDecodeError as e:
                print(f"Error reading JSON file {file}: {str(e)}")
                continue
            except KeyError as e:
                print(f"Missing required field in {file}: {str(e)}")
                continue
        
        if station_data:
            merged_data[(station_name, station_key)] = station_data
    
    return merged_data

def save_to_mongodb(data: Dict, db: MongoClient) -> None:
    collection = db.weather_db.weather_data
    success_count = 0
    error_count = 0
    
    for station_data in data.values():
        try:
            # First, get the existing document if it exists
            existing_doc = collection.find_one({"key": station_data["key"]})
            
            if existing_doc:
                # Update existing document
                # 1. Update metadata if changed
                metadata_update = {
                    "name": station_data["name"],
                    "owner": station_data["owner"],
                    "ownerCategory": station_data["ownerCategory"],
                    "measuringStations": station_data["measuringStations"],
                    "height": station_data["height"],
                    "latitude": station_data["latitude"],
                    "longitude": station_data["longitude"],
                    "last_updated": station_data["last_updated"]
                }
                
                # 2. For each parameter, append new values if they don't exist
                for new_param in station_data["parameters"]:
                    param_key = new_param["key"]
                    new_period = new_param["periods"][0]
                    
                    # Find if this parameter exists
                    existing_param = next(
                        (p for p in existing_doc["parameters"] if p["key"] == param_key),
                        None
                    )
                    
                    if existing_param:
                        # Check if this period's values already exist
                        existing_period = next(
                            (p for p in existing_param["periods"] if p["from"] == new_period["from"]),
                            None
                        )
                        
                        if not existing_period:
                            # Append new period
                            collection.update_one(
                                {"key": station_data["key"], "parameters.key": param_key},
                                {
                                    "$push": {"parameters.$.periods": new_period},
                                    "$set": metadata_update
                                }
                            )
                    else:
                        # Add new parameter
                        collection.update_one(
                            {"key": station_data["key"]},
                            {
                                "$push": {"parameters": new_param},
                                "$set": metadata_update
                            }
                        )
            else:
                # Insert new document
                collection.insert_one(station_data)
                success_count += 1
                
        except Exception as e:
            print(f"Error saving station {station_data['key']}: {str(e)}")
            error_count += 1
    
    print(f"Successfully saved {success_count} stations")
    if error_count > 0:
        print(f"Failed to save {error_count} stations")

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
    
    if not merged_data:
        print("No valid data to save")
        return
    
    # Connect to MongoDB and save data
    print("Saving to MongoDB...")
    try:
        client = get_mongodb_client()
        save_to_mongodb(merged_data, client)
    except ConnectionError as e:
        print(f"Failed to connect to MongoDB: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main() 