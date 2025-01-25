import pandas as pd

# Load the CSV file into a DataFrame
file_path = 'transit_data_january_2024.csv'
df = pd.read_csv(file_path)

# View the shape of the DataFrame (number of rows and columns)
print(f"Shape of the file: {df.shape}")

# View the first 5 records to get an idea of the data
print(df.head())
print(df.tail())
