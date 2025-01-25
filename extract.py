from functions import fetch_transit_data, save_to_csv
import os

def main():
    # Base URL of the API
    base_url = "https://data.ny.gov/resource/wujg-7c2s.json"

    # Define the date range
    start_date = "01/01/2024"
    end_date = "01/31/2024"

    # Fetch the data
    print("Fetching transit data...")
    try:
        limit = 5000000  # Set the desired limit
        data = fetch_transit_data(base_url, start_date, end_date, limit)
        print(f"Fetched {len(data)} records.")

        # Define the output file path to save on Desktop
        output_file = os.path.expanduser("~/Desktop/capstone/transit_data_january_2024.csv")
        print(f"Saving data to {output_file}...")

        # Save the data to a CSV file
        save_to_csv(data, output_file)
        print("Data saved successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
    