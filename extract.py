from functions import *

import os
import time


from datetime import datetime, timedelta

def main():
    # Record the start time
    start_time = time.time()

    logger.log_text("Starting main function...")
    # Base URL of the API
    base_url = "https://data.ny.gov/resource/wujg-7c2s.json"

    # Define the start and end dates for data availability (configurable)
    start_date = "07/2020"  # Start from July 2024
    end_date = datetime.now().strftime("%m/%Y")   # End at current month

    # Define your GCS bucket name (configurable)
    gcs_bucket_name = "nyc_subway_data"

    # Get existing files from the GCS bucket
    existing_files = get_existing_files_gcs(gcs_bucket_name, file_extension=".parquet")
    existing_files_no_ext = [os.path.splitext(blob)[0] for blob in existing_files]
    logger.log_text(f"Existing files in GCS: {existing_files_no_ext}")

    # Generate the full list of months to process
    all_months = generate_date_range(start_date, end_date)
    logger.log_text(f"All months to process: {all_months}")

    # Identify missing months
    missing_months = [month for month in all_months if f"{month}.parquet" not in existing_files]
    logger.log_text(f"Missing months to process: {missing_months}")

    if existing_files:
        most_recent_file = get_most_recent_file(gcs_bucket_name, file_extension=".parquet")

        # Process the most recent Parquet file (update with new entries)
        if most_recent_file:
            logger.log_text(f"The most recent Parquet file is: {most_recent_file}")
            month = most_recent_file.split(".")[0]  # Extract MM-YYYY from the filename

            # Get the latest entry in the existing Parquet file
            latest_entry_in_gcs = get_latest_entry_from_gcs(gcs_bucket_name, most_recent_file)

            # Get the latest entry in the API
            latest_entry_in_api = get_latest_entry_from_api(base_url)

            if not latest_entry_in_api:
                # logger.error("Failed to get the latest entry from the API. Skipping update.")
                logger.log_text("Failed to get the latest entry from the API. Skipping update.", severity="ERROR")
            else:
                # Compare the latest entries
                if latest_entry_in_gcs >= latest_entry_in_api:
                    logger.log_text(f"No new data for {month}. Skipping update.")

                else:
                    # Fetch new data from the API
                    logger.log_text(f"Fetching new data for {month}...")
                    try:
                        # Get the latest timestamp from the most recent Parquet file
                        latest_timestamp = latest_entry_in_gcs

                        # If the latest timestamp is not the last day of the month, fetch data for the next day
                        last_day_of_month = (latest_timestamp.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                        if latest_timestamp < last_day_of_month:
                            start_date_str = (latest_timestamp + timedelta(days=1)).strftime("%m/%d/%Y")
                            end_date_str = datetime.strptime(start_date_str, "%m/%d/%Y").replace(day=28) + timedelta(days=4)
                            end_date_str = end_date_str.replace(day=1) - timedelta(days=1)  # Get the last day of the month
                            end_date_str = end_date_str.strftime("%m/%d/%Y")
                            logger.log_text(f"Fetching data from {start_date_str} to {end_date_str}...")

                            data = fetch_transit_data(base_url, start_date_str, end_date_str, limit=5000000)
                            logger.log_text(f"Fetched {len(data)} new records for {month}.")

                            # Append new data to the existing Parquet file in GCS
                            append_to_parquet_in_gcs(gcs_bucket_name, data, most_recent_file, timeout=600)
                            logger.log_text(f"Updated {most_recent_file} with new data.")

                        else:
                            # If the latest timestamp is the last day of the month, no new data to fetch
                            logger.log_text(f"No new data for {month}. Latest timestamp is already the last day of the month.")

                    except Exception as e:
                        logger.log_text(f"An error occurred while updating {most_recent_file}: {e}", severity="ERROR")
                        raise
    else:
        logger.log_text("No Parquet files found in the GCS bucket. Fetching data for all months.")

    if missing_months:
        for month in missing_months:
            try:
                fetch_and_upload_month_data(base_url, gcs_bucket_name, month)
            except Exception as e:
                logger.log_text(f"An error occurred while processing month {month}: {e}", severity="ERROR")
    else:
        logger.log_text("No missing months to process.")

    # Record the end time
    end_time = time.time()

    # Calculate the time taken
    time_taken = (end_time - start_time)/60
    logger.log_text(f"Time Taken: {time_taken:.2f} minutes")

if __name__ == "__main__":
    logger.log_text("Script started.")
    main()
    logger.log_text("Script finished.")
