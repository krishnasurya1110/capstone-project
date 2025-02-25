from logging_config import *
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from google.cloud import storage
from io import StringIO
import os
from datetime import datetime
import json

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCS_CREDENTIALS_PATH")

# Load configuration from a JSON file
with open("config.json") as config_file:
    config = json.load(config_file)

# Validate configuration
def validate_config(config):
    required_keys = ["openmeteo_params", "gcs_bucket_name"] 
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required key in config: {key}")

validate_config(config)

# Validate date format and range
def validate_dates(start_date, end_date):
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        if start > end:
            raise ValueError("Start date must be before end date.")
    except ValueError as e:
        logger.log_text(f"Invalid date format: {e}", severity="ERROR")
        raise

validate_dates(config["openmeteo_params"]["start_date"], config["openmeteo_params"]["end_date"])

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Fetch weather data from Open-Meteo API
def fetch_weather_data(url, params):
    """
    Fetches weather data from the Open-Meteo API.

    Args:
        url (str): The API endpoint URL.
        params (dict): Parameters for the API request.

    Returns:
        list: List of responses from the API.
    """
    try:
        logger.log_text("Fetching weather data from Open-Meteo API...")
        responses = openmeteo.weather_api(url, params=params)
        return responses
    except Exception as e:
        logger.log_text(f"Error fetching data from Open-Meteo API: {e}", severity="ERROR")
        raise

# Process weather data into a DataFrame
def process_weather_data(response):
    """
    Processes the API response into a pandas DataFrame.

    Args:
        response: The API response object.

    Returns:
        pd.DataFrame: DataFrame containing hourly weather data.
    """
    try:
        logger.log_text("Processing weather data...")
        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
            "apparent_temperature": hourly.Variables(2).ValuesAsNumpy(),
            "precipitation": hourly.Variables(3).ValuesAsNumpy(),
            "rain": hourly.Variables(4).ValuesAsNumpy(),
            "snowfall": hourly.Variables(5).ValuesAsNumpy()
        }
        df = pd.DataFrame(data=hourly_data)
        return df
    except Exception as e:
        logger.log_text(f"Error processing weather data: {e}",  severity="ERROR")
        raise

# Check if a blob exists in the GCS bucket
def blob_exists(bucket_name, blob_name):
    """
    Checks if a blob exists in the GCS bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob to check.

    Returns:
        bool: True if the blob exists, False otherwise.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logger.log_text(f"Error checking if blob exists: {e}", severity="ERROR")
        raise

# Upload data to Google Cloud Storage
def upload_to_gcs(bucket_name, destination_blob_name, data):
    """
    Uploads data to a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        destination_blob_name (str): Path to the destination blob.
        data (str): Data to upload as a string.

    Returns:
        None
    """
    try:
        logger.log_text(f"Uploading data to GCS bucket: {bucket_name}/{destination_blob_name}...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type="text/csv")
        logger.log_text(f"Data uploaded to {destination_blob_name} in bucket {bucket_name}.")
    except Exception as e:
        logger.log_text(f"Error uploading data to GCS: {e}", severity="ERROR")
        raise

# Main function to orchestrate the workflow
def main():
    try:
        # Fetch weather data
        responses = fetch_weather_data(config["openmeteo_url"], config["openmeteo_params"])
        response = responses[0]

        # Process weather data
        df = process_weather_data(response)

        # Split the DataFrame by month
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        grouped = df.groupby([df.index.year, df.index.month])

        # Get start and end dates from config
        start_date = config["openmeteo_params"]["start_date"]
        end_date = config["openmeteo_params"]["end_date"]
        print(f"Checking for missing files between {start_date} and {end_date}...")

        # Upload each month's data to GCS
        for (year, month), month_data in grouped:
            # Reset the index to include the 'date' column in the CSV
            month_data = month_data.reset_index()
            destination_blob_name = f"weather/{month:02d}-{year}.csv"
            
            # Check if the blob already exists
            if not blob_exists(config["gcs_bucket_name"], destination_blob_name):
                print(f"Uploading {destination_blob_name}...")
                csv_buffer = StringIO()
                month_data.to_csv(csv_buffer, index=False)
                upload_to_gcs(config["gcs_bucket_name"], destination_blob_name, csv_buffer.getvalue())
                print(f"{destination_blob_name} uploaded successfully.")
            else:
                # Skip printing for files that already exist
                pass

    except Exception as e:
        logger.log_text(f"An error occurred in the main workflow: {e}", severity="ERROR")
        raise

if __name__ == "__main__":
    main()
