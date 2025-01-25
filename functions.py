import requests
import csv
import os
from datetime import datetime, timedelta

def get_existing_files(folder_path):
    """
    Get a list of existing CSV files in the folder.

    Parameters:
    - folder_path (str): Path to the folder containing CSV files.

    Returns:
    - set: A set of existing filenames in 'MM-YYYY' format.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    return {
        os.path.splitext(file)[0]
        for file in os.listdir(folder_path)
        if file.endswith(".csv")
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
