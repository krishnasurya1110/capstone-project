from logging_config import logger

import requests
import os
from datetime import datetime, timedelta
from google.cloud import storage
from io import StringIO
import csv

def get_existing_files_gcs(bucket_name):
    """
    Get a list of existing CSV filenames in the GCS bucket.

    Parameters:
    - bucket_name (str): Name of the GCS bucket.

    Returns:
    - set: A set of existing filenames in 'MM-YYYY' format.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    return {
        os.path.splitext(blob.name)[0]  # Extract filename without extension
        for blob in blobs
        if blob.name.endswith(".csv")
    }

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

def upload_csv_to_gcs(bucket_name, data, destination_blob_name, timeout=300):
    """
    Upload a CSV data string directly to Google Cloud Storage.

    Parameters:
    - bucket_name (str): Name of the GCS bucket.
    - data (list of dict): The data to write to the CSV file.
    - destination_blob_name (str): Destination path in the GCS bucket.
    - timeout (int): Timeout in seconds for the upload operation.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert data to CSV format
    output = StringIO()
    if data:
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    # Upload CSV content to GCS with timeout
    blob.upload_from_string(output.getvalue(), content_type="text/csv", timeout=timeout)
    print(f"File uploaded to {destination_blob_name} in bucket {bucket_name}.")

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

    # Build the query with a limit
    query = f"""
        SELECT 
            transit_timestamp,
            transit_mode,
            station_complex_id,
            station_complex,
            borough,
            payment_method,
            fare_class_category,
            ridership,
            transfers,
            latitude,
            longitude,
            georeference
        WHERE
            transit_timestamp >= '{start_date_iso}' AND
            transit_timestamp <= '{end_date_iso}'
        ORDER BY
            transit_timestamp
        LIMIT {limit}
    """
    url = f"{base_url}?$query={query}"

    # Fetch data
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

    return response.json()

def get_latest_entry_from_gcs(bucket_name, blob_name):
    """
    Get the latest entry (transit_timestamp) from an existing CSV file in GCS.

    Parameters:
    - bucket_name (str): Name of the GCS bucket.
    - blob_name (str): Name of the CSV file in GCS.

    Returns:
    - datetime: The latest transit_timestamp in the CSV file, or None if the file is empty.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not blob.exists():
        return None

    # Download the CSV content
    content = blob.download_as_text()
    reader = csv.DictReader(StringIO(content))
    rows = list(reader)

    if not rows:
        return None

    # Get the latest transit_timestamp
    latest_timestamp_str = max(row["transit_timestamp"] for row in rows)
    latest_timestamp = datetime.strptime(latest_timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
    return latest_timestamp

def append_to_csv_in_gcs(bucket_name, data, blob_name, timeout=300):
    """
    Append new data to an existing CSV file in GCS.

    Parameters:
    - bucket_name (str): Name of the GCS bucket.
    - data (list of dict): The new data to append.
    - blob_name (str): Name of the CSV file in GCS.
    - timeout (int): Timeout in seconds for the upload operation.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download existing content
    existing_content = blob.download_as_text()
    existing_rows = list(csv.DictReader(StringIO(existing_content)))

    # Append new data
    updated_rows = existing_rows + data

    # Convert updated data to CSV format
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(updated_rows)

    # Upload updated content to GCS
    blob.upload_from_string(output.getvalue(), content_type="text/csv", timeout=timeout)
    print(f"Appended new data to {blob_name} in bucket {bucket_name}.")

def get_most_recent_csv_file(bucket_name):
    """
    Get the most recent MM-YYYY.csv file from the GCS bucket.

    Parameters:
    - bucket_name (str): Name of the GCS bucket.

    Returns:
    - str: The filename of the most recent CSV file (e.g., "12-2023.csv"), or None if no files exist.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    # Extract filenames and filter for CSV files
    csv_files = [
        blob.name for blob in blobs if blob.name.endswith(".csv") and "-" in blob.name
    ]

    if not csv_files:
        return None

    # Find the most recent file based on the filename (MM-YYYY)
    most_recent_file = max(csv_files, key=lambda x: datetime.strptime(x.split(".")[0], "%m-%Y"))
    return most_recent_file

def get_latest_date_from_api(base_url):
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
