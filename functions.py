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

def save_to_csv(data, file_name):
    """
    Save the data to a CSV file.

    Parameters:
    - data (list): The data to save.
    - file_name (str): The output file name.
    """
    if not data:
        raise ValueError("No data to save.")

    # Extract headers from the first record
    headers = data[0].keys()

    # Write to CSV
    with open(file_name, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
