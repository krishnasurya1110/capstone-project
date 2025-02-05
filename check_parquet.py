import pandas as pd

# #------------
# # Upload to gcp
# # Read the Parquet file
# df = pd.read_parquet('datasets/gcp/12-2024.parquet')

# # Display the DataFrame
# print(df)
# # #------------

# # ORIGINAL
# # Read the Parquet file
# df = pd.read_parquet('datasets/12-2024_original.parquet')

# # Display the DataFrame
# print(df)

# #-------------

# # FILTERED
# # Read the Parquet file
# df = pd.read_parquet('datasets/12-2024_filtered.parquet')

# # Display the DataFrame
# print(df)

# #--------------

# # APPENDED
# # Read the Parquet file
# df = pd.read_parquet('datasets/12-2024_appended.parquet')

# # Display the DataFrame
# print(df)

# CHECK UPLOADED AND DOWNLOADED
df_up = pd.read_parquet('datasets/08-2024_up.parquet')
print("df_up")
print(df_up)

df_down = pd.read_parquet('datasets/08-2024_down.parquet')
print("df_down")
print(df_down)

# #---------------

# # COUNT MATCHING ROWS

# # Convert the 'transit_timestamp' column to datetime
# df['transit_timestamp'] = pd.to_datetime(df['transit_timestamp'])

# # Define the target date (as a string or datetime.date object)
# target_date = '2024-12-31'  # or use pd.to_datetime('2024-12-31').date()

# # Condition: Match only the date part (ignoring time)
# condition = df['transit_timestamp'].dt.date == pd.to_datetime(target_date).date()

# # Count the number of rows that match the condition
# num_matching_rows = condition.sum()
# print(f"Number of rows matching the date {target_date}: {num_matching_rows}")

# #----------

# # REMOVE MATCHING ROWS

# # Option 1: Filter the DataFrame to keep only rows that do NOT match the condition
# df_filtered = df[~condition]

# # # Option 2: Use the `drop` method to delete rows that match the condition
# # df_dropped = df_original.drop(df_original[condition].index)

# # Display the modified DataFrame
# print("\nDataFrame after deleting matching rows:")
# print(df_filtered)  # or print(df_dropped)

# df_filtered.to_parquet('datasets/12-2024_filtered.parquet', index=False)

