from functions import *

import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging_v2.handlers import setup_logging
import logging
import sys

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up GCP Logging (only once)
client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)
setup_logging(handler)

# Use Python's standard logging module
logger = logging.getLogger(__name__)

# Add a StreamHandler to print logs to the console (only if not already added)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

# Set logging level to INFO
logger.setLevel(logging.INFO)


def main():
    logger.info("Starting main function...")
    # Base URL of the API
    base_url = "https://data.ny.gov/resource/wujg-7c2s.json"

    # Define the start and end dates for data availability (configurable)
    start_date = "07/2020"  # Start from July 2020
    end_date = datetime.now().strftime("%m/%Y")   # End at current month

    # Define your GCS bucket name (configurable)
    gcs_bucket_name = "testdata_mta"

    # Get existing files from the GCS bucket
    existing_files = get_existing_files_gcs(gcs_bucket_name, file_extension=".parquet")
    existing_files_no_ext = [os.path.splitext(blob)[0] for blob in existing_files]
    logger.info(f"Existing files in GCS: {existing_files_no_ext}")

    # Generate the full list of months to process
    all_months = generate_date_range(start_date, end_date)
    logger.info(f"All months to process: {all_months}")

    # Identify missing months
    missing_months = [month for month in all_months if f"{month}.parquet" not in existing_files]
    logger.info(f"Missing months to process: {missing_months}")

    # Get the most recent Parquet file in the GCS bucket
    most_recent_file = get_most_recent_file(gcs_bucket_name, file_extension=".parquet")
    if not most_recent_file:
        logger.info("No Parquet files found in the GCS bucket. Fetching data for all months.")

    # Process missing months in parallel (create new Parquet files)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_and_upload_month_data, base_url, gcs_bucket_name, month) for month in missing_months]
        for future in as_completed(futures):
            try:
                future.result()  # This will raise any exceptions caught in the thread
            except Exception as e:
                logger.error(f"An error occurred in a thread: {e}")


    # Process the most recent Parquet file (update with new entries)
    if most_recent_file:
        logger.info(f"The most recent Parquet file is: {most_recent_file}")
        month = most_recent_file.split(".")[0]  # Extract MM-YYYY from the filename
        # Get the latest entry in the existing Parquet file
        latest_entry_in_gcs = get_latest_entry_from_gcs(gcs_bucket_name, most_recent_file)
        if not latest_entry_in_gcs:
            logger.info(f"No data found in {most_recent_file}. Skipping update.")
        else:
            # Get the latest entry from the API
            latest_entry_in_api = get_latest_entry_from_api(base_url)

            if not latest_entry_in_api:
                logger.error("Failed to get the latest entry from the API. Skipping update.")
            else:
                # Compare the latest entries
                if latest_entry_in_gcs >= latest_entry_in_api:
                    logger.info(f"No new data for {month}. Skipping update.")
                else:
                    # Fetch new data from the API
                    logger.info(f"Fetching new data for {month}...")
                    try:
                        # Get the latest timestamp from the most recent Parquet file
                        latest_timestamp = latest_entry_in_gcs

                        # If the latest timestamp is not the last day of the month, fetch data for the next day
                        last_day_of_month = (latest_timestamp.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                        if latest_timestamp < last_day_of_month:
                            start_date_str = (latest_timestamp + timedelta(days=1)).strftime("%m/%d/%Y")
                            end_date_str = latest_entry_in_api.strftime("%m/%d/%Y")
                            logger.info(f"Fetching data from {start_date_str} to {end_date_str}...")
                        else:
                            # If the latest timestamp is the last day of the month, no new data to fetch
                            logger.info(f"No new data for {month}. Latest timestamp is already the last day of the month.")
                            return

                        data = fetch_transit_data(base_url, start_date_str, end_date_str, limit=5000000)
                        logger.info(f"Fetched {len(data)} new records for {month}.")

                        # Append new data to the existing Parquet file in GCS
                        append_to_parquet_in_gcs(gcs_bucket_name, data, most_recent_file, timeout=600)
                        logger.info(f"Updated {most_recent_file} with new data.")

                    except Exception as e:
                        logger.error(f"An error occurred while updating {most_recent_file}: {e}")
                        raise


if __name__ == "__main__":
    logger.info("Script started.")
    main()
    logger.info("Script finished.")
