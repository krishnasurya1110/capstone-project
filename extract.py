import os
from datetime import datetime, timedelta
from functions import fetch_transit_data, save_to_csv, get_existing_files, generate_date_range

def main():
    # Base URL of the API
    base_url = "https://data.ny.gov/resource/wujg-7c2s.json"

    # Define the date range for data availability
    start_date = "07/2020"  # Start from July 2020
    today = datetime.today()
    previous_month = today.replace(day=1) - timedelta(days=1)  # Get the last day of the previous month
    end_date = previous_month.strftime("%m/%Y")

    # Define the datasets folder
    datasets_folder = os.path.expanduser("datasets")

    # Get existing files
    existing_files = get_existing_files(datasets_folder)

    # Generate the full list of months to process
    all_months = generate_date_range(start_date, end_date)
    missing_months = [month for month in all_months if month not in existing_files]

    if not missing_months:
        print("All data is already downloaded.")
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

            # Define the output file path
            output_file = os.path.join(datasets_folder, f"{month}.csv")

            # Save the data to a CSV file
            print(f"Saving data to {output_file}...")
            save_to_csv(data, output_file)
            print(f"Data for {month} saved successfully.")
        except Exception as e:
            print(f"An error occurred for {month}: {e}")

if __name__ == "__main__":
    main()
