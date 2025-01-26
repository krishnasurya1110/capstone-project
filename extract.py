from datetime import datetime, timedelta
from functions import get_existing_files_gcs, generate_date_range, upload_csv_to_gcs
from functions import fetch_transit_data

def main():
    # Base URL of the API
    base_url = "https://data.ny.gov/resource/wujg-7c2s.json"

    # Define the date range for data availability
    start_date = "01/2024"  # Start from Jan 2024
    today = datetime.today()
    previous_month = today.replace(day=1) - timedelta(days=1)  # Get the last day of the previous month
    end_date = previous_month.strftime("%m/%Y")

    # Define your GCS bucket name
    gcs_bucket_name = "capstone-project-group7"

    # Get existing files from the GCS bucket
    existing_files = get_existing_files_gcs(gcs_bucket_name)

    # Generate the full list of months to process
    all_months = generate_date_range(start_date, end_date)
    missing_months = [month for month in all_months if month not in existing_files]

    if not missing_months:
        print("All data is already uploaded to GCS.")
        return

    print(f"Missing months: {missing_months}")

    # Process each missing month
    for month in missing_months:
        month_num, year = month.split("-")
        start_date = f"{month_num}/01/{year}"
        end_date = datetime.strptime(start_date, "%m/%d/%Y").replace(day=28) + timedelta(days=4)
        end_date = end_date.replace(day=1) - timedelta(days=1)  # Get the last day of the month
        end_date = end_date.strftime("%m/%d/%Y")

        # Fetch the data
        print(f"Fetching data for {month}...")
        try:
            limit = 5000000  # Set the desired limit
            data = fetch_transit_data(base_url, start_date, end_date, limit)
            print(f"Fetched {len(data)} records for {month}.")

            # Upload the data directly to GCS as a CSV
            destination_blob_name = f"{month}.csv"
            print(f"Uploading data for {month} to Google Cloud Storage...")
            upload_csv_to_gcs(gcs_bucket_name, data, destination_blob_name, timeout=600)  # 10 minutes timeout

        except Exception as e:
            print(f"An error occurred for {month}: {e}")

if __name__ == "__main__":
    main()
