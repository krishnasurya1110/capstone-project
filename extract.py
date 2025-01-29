from logging_config import logger

from datetime import datetime, timedelta
from functions import get_existing_files_gcs, generate_date_range, upload_csv_to_gcs, fetch_transit_data, get_latest_date_from_api, get_latest_entry_from_gcs, append_to_csv_in_gcs, get_most_recent_csv_file

def main():
    # Base URL of the API
    base_url = "https://data.ny.gov/resource/wujg-7c2s.json"

    # Define the start date for data availability
    start_date = "07/2020"  # Start from July 2020

    # Get the latest date from the API
    try:
        latest_date = get_latest_date_from_api(base_url)
        end_date = latest_date.strftime("%m/%Y")
    except Exception as e:
        print(f"Failed to get the latest date from the API: {e}")
        return

    # Define your GCS bucket name
    gcs_bucket_name = "capstone-project-group7"

    # Get the most recent CSV file in the GCS bucket
    most_recent_csv_file = get_most_recent_csv_file(gcs_bucket_name)
    if most_recent_csv_file:
        print(f"The most recent CSV file is: {most_recent_csv_file}")
    else:
        print("No CSV files found in the GCS bucket.")

    # Get existing files from the GCS bucket
    existing_files = get_existing_files_gcs(gcs_bucket_name)

    # Generate the full list of months to process
    all_months = generate_date_range(start_date, end_date)
    missing_months = [month for month in all_months if month not in existing_files]

    # Process missing months (create new CSV files)
    for month in missing_months:
        month_num, year = month.split("-")
        start_date_str = f"{month_num}/01/{year}"
        end_date_str = datetime.strptime(start_date_str, "%m/%d/%Y").replace(day=28) + timedelta(days=4)
        end_date_str = end_date_str.replace(day=1) - timedelta(days=1)  # Get the last day of the month
        end_date_str = end_date_str.strftime("%m/%d/%Y")

        print(f"Fetching data for {month}...")
        try:
            limit = 5000000  # Set the desired limit
            data = fetch_transit_data(base_url, start_date_str, end_date_str, limit)
            print(f"Fetched {len(data)} records for {month}.")

            # Upload the data directly to GCS as a CSV
            destination_blob_name = f"{month}.csv"
            print(f"Uploading data for {month} to Google Cloud Storage...")
            upload_csv_to_gcs(gcs_bucket_name, data, destination_blob_name, timeout=600)  # 10 minutes timeout

        except Exception as e:
            print(f"An error occurred for {month}: {e}")

    # Process the most recent CSV file (update with new entries)
    if most_recent_csv_file:
        month = most_recent_csv_file.split(".")[0]  # Extract MM-YYYY from the filename
        # Get the latest entry in the existing CSV file
        latest_entry_in_gcs = get_latest_entry_from_gcs(gcs_bucket_name, most_recent_csv_file)
        if not latest_entry_in_gcs:
            print(f"No data found in {most_recent_csv_file}. Skipping update.")
        else:
            # Get the latest entry from the API
            latest_entry_in_api = get_latest_date_from_api(base_url)

            # Compare the latest entries
            if latest_entry_in_gcs >= latest_entry_in_api:
                print(f"No new data for {month}. Skipping update.")
            else:
                # Fetch new data from the API
                print(f"Fetching new data for {month}...")
                try:
                    start_date_str = latest_entry_in_gcs.strftime("%m/%d/%Y")
                    end_date_str = latest_entry_in_api.strftime("%m/%d/%Y")
                    data = fetch_transit_data(base_url, start_date_str, end_date_str, limit=5000000)
                    print(f"Fetched {len(data)} new records for {month}.")

                    # Append new data to the existing CSV file in GCS
                    append_to_csv_in_gcs(gcs_bucket_name, data, most_recent_csv_file, timeout=600)
                    print(f"Updated {most_recent_csv_file} with new data.")

                except Exception as e:
                    print(f"An error occurred while updating {most_recent_csv_file}: {e}")

if __name__ == "__main__":
    main()
