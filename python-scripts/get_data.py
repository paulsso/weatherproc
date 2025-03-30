import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_weather_data() -> bytes:
    base_url = os.getenv("SMHI_API_URL")
    parameter = os.getenv("SMHI_PARAMETER")
    station = os.getenv("SMHI_STATION")
    period = os.getenv("SMHI_PERIOD")
    
    endpoint = f"parameter/{parameter}/station/{station}/period/{period}/data.csv"
    headers = {
        "User-Agent": "curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.15.3 zlib/1.2.3 libidn/1.18 libssh2/1.4.2",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate"
    }
    
    response = requests.get(f"{base_url}/{endpoint}", headers=headers)
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert isinstance(response.content, bytes)
    return response.content

if __name__ == "__main__":
    data = get_weather_data()
    with open("data.csv", "wb") as f:
        f.write(data) 