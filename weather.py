import requests
import json
import os
from urllib.parse import quote_plus

# Define the base API URL and API key
base_api_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
api_key = "2VHWXMPKFJBKJALVYQFYY3GY8"

# List of boroughs to fetch weather data for
boroughs = ["Manhattan", "Brooklyn", "Bronx", "Queens", "Staten Island"]

# Define date range
start_date = "2024-01-01"
end_date = "2024-12-31"

# Define the elements and include parameters as per your sample URL
elements = "datetime,datetimeEpoch,name,address,latitude,longitude,tempmax,tempmin,temp,feelslikemax,feelslikemin,feelslike,dew,humidity,precip,preciptype,snow,snowdepth,windgust,windspeed,winddir,pressure,visibility,solarenergy,sunrise,sunset,conditions,description,icon,source"
include = "fcst,obs,histfcst,stats"

# Path to save the files
output_path = "/Users/monicamuniraj/Documents/capstone/capstone-project/datasets"

# Ensure the output directory exists
os.makedirs(output_path, exist_ok=True)

# Fetch and save weather data for each borough
for borough in boroughs:
    # URL-encode the borough name to handle spaces (like in Staten Island)
    encoded_borough = quote_plus(borough)

    # Construct the full API URL with all parameters for the borough
    api_url = f"{base_api_url}/{encoded_borough}/{start_date}/{end_date}?unitGroup=us&elements={elements}&include={include}&key={api_key}&contentType=json"

    try:
        # Make the API request
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for HTTP issues
        weather_data = response.json()  # Parse the JSON response

        # Construct the file name and path
        file_name = f"weather_{encoded_borough.lower()}.json"
        file_path = os.path.join(output_path, file_name)

        # Save the data to a file
        with open(file_path, "w") as file:
            json.dump(weather_data, file, indent=4)
            print(f"Weather data for {borough} saved to '{file_path}'")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data for {borough}: {e}")
