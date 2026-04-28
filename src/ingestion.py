import requests
import pandas as pd
import time
import os
import logging
from datetime import datetime, timedelta

# Configuration Limits
RATE_LIMIT_DELAY = 20.0
RATE_LIMIT_EXCEEDED_DELAY = 600
MAX_RETRIES = 3  # As per task requirement

logger = logging.getLogger(__name__)

# --- 1. HISTORICAL FUNCTION ---
def fetch_historical(city_name, latitude, longitude, start_date, end_date, variables):
    # Date range validation
    if datetime.strptime(start_date, '%Y-%m-%d') >= datetime.strptime(end_date, '%Y-%m-%d'):
        raise ValueError("Start date must be before end date.")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude, "longitude": longitude,
        "start_date": start_date, "end_date": end_date,
        "daily": variables, "timezone": "auto"
    }
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if "daily" not in data:
                    raise ValueError(f"Malformed response for {city_name}")
                
                df = pd.DataFrame(data["daily"])
                df.insert(0, 'city', city_name.lower())
                # Keep 'time' column name as expected by pipeline.py
                
                # Save locally
                df.to_csv(f"{city_name.lower()}_historical.csv", index=False)
                return df
            
            elif response.status_code == 429:
                print(f"!!! Rate limit exceeded for {city_name}. Waiting {RATE_LIMIT_EXCEEDED_DELAY}s...")
                time.sleep(RATE_LIMIT_EXCEEDED_DELAY)
                retries += 1
            else:
                response.raise_for_status()
                
        except Exception as e:
            retries += 1
            # Exponential backoff: 2, 4, 8 seconds...
            wait_time = 2 ** retries 
            print(f"Error fetching historical data for {city_name}: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
            
    raise Exception(f"Failed to fetch historical data for {city_name} after {MAX_RETRIES} retries.")

# --- 2. FORECAST FUNCTION ---
def fetch_forecast(city_name, latitude, longitude, variables):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude, "longitude": longitude,
        "daily": variables, "timezone": "auto",
        "forecast_days": 7
    }
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if "daily" not in data:
                    raise ValueError(f"Empty or malformed forecast for {city_name}")
                
                df = pd.DataFrame(data["daily"])
                df.insert(0, 'city', city_name.lower())
                # Keep 'time' column name as expected by pipeline.py
                
                df.to_csv(f"{city_name.lower()}_forecast.csv", index=False)
                return df
            else:
                response.raise_for_status()
        except Exception as e:
            retries += 1
            time.sleep(2 ** retries)
            print(f"Forecast error for {city_name}: {e}")
            
    return None

# --- 3. BATCH PROCESSING FUNCTION ---
def fetch_all_cities(cities_config, start_date, end_date, variables):
    print(f"Starting ingestion process for {len(cities_config)} cities...")
    all_city_data = {}
    
    for city in cities_config:
        city_name = city['name']
        
        # Fetch Historical
        if not os.path.exists(f"{city_name.lower()}_historical.csv"):
            print(f"Fetching historical data for: {city_name}...")
            df_hist = fetch_historical(city_name, city['latitude'], city['longitude'], start_date, end_date, variables)
            all_city_data[city_name] = df_hist
            time.sleep(RATE_LIMIT_DELAY) 
        
        # Fetch Forecast
        print(f"Fetching forecast for: {city_name}...")
        fetch_forecast(city_name, city['latitude'], city['longitude'], variables)
        time.sleep(1)

    print("✅ INGESTION TASK COMPLETED!")
    return all_city_data