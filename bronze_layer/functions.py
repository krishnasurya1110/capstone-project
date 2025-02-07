from logging_config import *

import requests
from datetime import datetime, timedelta
from google.cloud import storage
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def get_most_recent_file(bucket_name, file_extension=".parquet"):
    """
    Get the most recent file in the GCS bucket with the specified file extension.
    If no files are found, return None.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # List all files in the bucket with the specified extension
    blobs = bucket.list_blobs()
    filtered_blobs = [blob for blob in blobs if blob.name.endswith(file_extension)]

    if not filtered_blobs:
        return None  # No files found with the specified extension

    # Sort files by their names (assuming filenames are in the format MM-YYYY.parquet)
    filtered_blobs.sort(key=lambda blob: blob.name, reverse=True)

    # Return the most recent file based on the filename
    return filtered_blobs[0].name


def get_existing_files_gcs(bucket_name, file_extension=".parquet"):
    """
    Get a list of existing files in the GCS bucket with the specified file extension.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # List all files in the bucket with the specified extension
    blobs = bucket.list_blobs()
    existing_files = [blob.name for blob in blobs if blob.name.endswith(file_extension)]

    return existing_files


def generate_date_range(start_date, end_date):
    """
    Generate a list of months between two dates in 'MM-YYYY' format.

    Parameters:
    - start_date (str): Start date in 'MM/YYYY' format.
    - end_date (str): End date in 'MM/YYYY' format.

    Returns:
    - list: List of months in 'MM-YYYY' format.
    """
    start = datetime.strptime(start_date, "%m/%Y")
    end = datetime.strptime(end_date, "%m/%Y")
    months = []

    while start <= end:
        months.append(start.strftime("%m-%Y"))
        start += timedelta(days=32)
        start = start.replace(day=1)  # Ensure we stay at the start of the month

    return months

def fetch_transit_data(base_url, start_date, end_date, limit=5000000):
    """
    Fetch transit data from the API for the given date range.

    Parameters:
    - base_url (str): The base URL of the API.
    - start_date (str): The start date in 'MM/DD/YYYY' format.
    - end_date (str): The end date in 'MM/DD/YYYY' format.
    - limit (int): The maximum number of rows to fetch.

    Returns:
    - list: The JSON response as a Python list.
    """
    # Convert dates to ISO 8601 format
    start_date_iso = f"{start_date[6:10]}-{start_date[0:2]}-{start_date[3:5]}T00:00:00"
    end_date_iso = f"{end_date[6:10]}-{end_date[0:2]}-{end_date[3:5]}T23:59:59"

    # Define the API endpoint with date filtering
    url = (
        f"{base_url}"
        f"?$where=transit_timestamp >= '{start_date_iso}' AND transit_timestamp <= '{end_date_iso}'"
        f" &$limit={limit}"
    )

    try:
        response = requests.get(url)

        # Handle the response
        if response.status_code == 200:
            return response.json()  # Return JSON response
        else:
            logger.log_text(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
    
    except requests.exceptions.RequestException as e:
        # logger.log_text(f"An error occurred during the request: {e}", severity=500)
        raise Exception(f"An error occurred during the request: {e}")
    

def json_to_parquet(data):
    """Convert JSON data to Parquet format."""
    df = pd.DataFrame(data)  # Convert JSON to DataFrame

    # Explicitly convert datetime columns
    if "transit_timestamp" in df.columns:
        df["transit_timestamp"] = pd.to_datetime(df["transit_timestamp"])

    table = pa.Table.from_pandas(df)  # Convert DataFrame to PyArrow Table
    buffer = BytesIO()
    pq.write_table(table, buffer)  # Write Parquet to in-memory buffer
    buffer.seek(0)
    return buffer

def upload_parquet_to_gcs(bucket_name, data, destination_blob_name, timeout=1000):
    """Upload Parquet data to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    parquet_buffer = json_to_parquet(data)  # Convert JSON to Parquet
    blob.upload_from_file(parquet_buffer, timeout=timeout)  # Upload Parquet file


def fetch_and_upload_month_data(base_url, gcs_bucket_name, month):
    """Fetch data for a specific month, convert to Parquet, and upload to GCS."""
    try:
        month_num, year = month.split("-")
        start_date_str = f"{month_num}/01/{year}"
        end_date_str = datetime.strptime(start_date_str, "%m/%d/%Y").replace(day=28) + timedelta(days=4)
        end_date_str = end_date_str.replace(day=1) - timedelta(days=1)  # Get the last day of the month
        end_date_str = end_date_str.strftime("%m/%d/%Y")

        logger.log_text(f"Fetching data for {month}...")
        limit = 3000000  # Set the desired limit
        data = fetch_transit_data(base_url, start_date_str, end_date_str, limit)
        logger.log_text(f"Fetched {len(data)} records for {month}.")

        # Check if data is empty
        if not data:
            # logger.warning(f"No data found for {month}. Skipping file creation...")
            logger.log_text(f"No data found for {month}. Skipping file creation...", severity="WARNING")
            return
        
        # Upload the data directly to GCS as a Parquet file
        destination_blob_name = f"{month}.parquet"
        logger.log_text(f"Uploading data for {month} to Google Cloud Storage...")
        upload_parquet_to_gcs(gcs_bucket_name, data, destination_blob_name, timeout=600)  # 10 minutes timeout

    except Exception as e:
        logger.log_text(f"An error occurred for {month}: {e}", severity="ERROR")

def get_latest_entry_from_gcs(bucket_name, blob_name):
    """
    Get the latest entry (transit_timestamp) from an existing Parquet file in GCS.

    Parameters:
    - bucket_name (str): Name of the GCS bucket.
    - blob_name (str): Name of the Parquet file in GCS.

    Returns:
    - datetime: The latest transit_timestamp in the Parquet file, or None if the file is empty.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not blob.exists():
        return None

    # Download the Parquet file content
    content = blob.download_as_bytes()
    df = pd.read_parquet(BytesIO(content))

    if df.empty or "transit_timestamp" not in df.columns:
        return None

    # Get the latest transit_timestamp
    latest_timestamp = df["transit_timestamp"].max()
    
    if pd.isnull(latest_timestamp):
        return None

    # Convert to datetime if needed
    if isinstance(latest_timestamp, str):
        latest_timestamp = datetime.strptime(latest_timestamp, "%Y-%m-%dT%H:%M:%S.%f")

    return latest_timestamp

def get_latest_entry_from_api(base_url):
    """
    Fetch the latest transit_timestamp from the API.

    Parameters:
    - base_url (str): The base URL of the API.

    Returns:
    - datetime: The latest date from the API.
    """
    # Build the query to get the latest transit_timestamp
    query = """
        SELECT 
            transit_timestamp
        ORDER BY
            transit_timestamp DESC
        LIMIT 1
    """
    url = f"{base_url}?$query={query}"

    # Fetch data from the API
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch latest date: {response.status_code}, {response.text}")

    # Parse the response
    data = response.json()
    if not data:
        raise Exception("No data returned from the API.")

    # Extract the latest transit_timestamp
    latest_date_str = data[0]['transit_timestamp']
    latest_date = datetime.strptime(latest_date_str, "%Y-%m-%dT%H:%M:%S.%f")
    return latest_date

def append_to_parquet_in_gcs(bucket_name, data, destination_blob_name, timeout=600):
    """
    Append new data to an existing Parquet file in GCS.
    """
    try:
        logger.log_text(f"Appending data to {destination_blob_name}...")
        # Download the existing Parquet file
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        if not blob.exists():
            logger.log_text(f"File {destination_blob_name} does not exist. Uploading as a new file.")
            upload_parquet_to_gcs(bucket_name, data, destination_blob_name, timeout)
            return

        # Download the existing Parquet file to an in-memory buffer
        existing_buffer = BytesIO()
        blob.download_to_file(existing_buffer)
        existing_buffer.seek(0)

        # Read the existing Parquet file into a Pandas DataFrame
        existing_table = pq.read_table(existing_buffer)
        existing_df = existing_table.to_pandas()

        # Convert new data to a DataFrame
        new_df = pd.DataFrame(data)

        # Append new data to the existing DataFrame
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # Convert the combined DataFrame to Parquet
        combined_buffer = json_to_parquet(combined_df.to_dict("records"))

        # Upload the updated Parquet file to GCS
        blob.upload_from_file(combined_buffer, timeout=timeout)
        logger.log_text(f"Successfully updated {destination_blob_name}.")
    except Exception as e:
        # logger.error(f"Error appending data to {destination_blob_name}: {e}")
        logger.log_text(f"Error appending data to {destination_blob_name}: {e}", severity="ERROR")
        raise
