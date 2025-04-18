from google.cloud import bigquery

def run_bigquery_sql():
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Step 1: Define the SQL query to clean the data and save it to a new table
    sql_query_clean_data = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.relevant_data` AS
    SELECT 
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', transit_timestamp) AS transit_timestamp,
        transit_mode,
        station_complex,
        borough,
        payment_method,
        fare_class_category,
        CAST(ridership AS FLOAT64) AS ridership,
        CAST(transfers AS FLOAT64) AS transfers
    FROM `lively-encoder-448916-d5.nyc_subway.nyc`;
    """

    # Execute the query for clean_data
    query_job_clean_data = client.query(sql_query_clean_data)
    query_job_clean_data.result()
    print("Data has been written to the table: lively-encoder-448916-d5.nyc_subway.relevant_data")

    # Step 2: Group the data by relevant columns and save to a new table
    sql_query_grouped_data = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.pm_grouped` AS
    SELECT 
        transit_timestamp,
        transit_mode,
        station_complex,
        borough,
        payment_method,
        SUM(CAST(ridership AS INT64)) AS ridership,
        SUM(CAST(transfers AS INT64)) AS transfer
    FROM `lively-encoder-448916-d5.nyc_subway.relevant_data`
    GROUP BY 
        transit_timestamp, 
        transit_mode, 
        station_complex, 
        borough, 
        payment_method;
    """

     # Execute the query for grouped_data
    query_job_grouped_data = client.query(sql_query_grouped_data)
    query_job_grouped_data.result()
    print("Data has been written to the table: lively-encoder-448916-d5.nyc_subway.pm_grouped")

    # Step 3: Ordinal encode categorical columns and save to a new table
    sql_query_ordinal_encoded = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.original_encoded_cols_data` AS
    SELECT 
        transit_timestamp,
        ridership,
        transfer,
        -- Use DENSE_RANK() for ordinal encoding
        transit_mode,
        DENSE_RANK() OVER (ORDER BY transit_mode) AS transit_mode_index,
        station_complex,
        DENSE_RANK() OVER (ORDER BY station_complex) AS station_complex_index,
        borough,
        DENSE_RANK() OVER (ORDER BY borough) AS borough_index,
        payment_method,
        DENSE_RANK() OVER (ORDER BY payment_method) AS payment_method_index
    FROM `lively-encoder-448916-d5.nyc_subway.pm_grouped`;
    """
    # Execute the query for ordinal_encoded
    query_job_ordinal_encoded = client.query(sql_query_ordinal_encoded)
    query_job_ordinal_encoded.result()
    print("Data has been written to the table: lively-encoder-448916-d5.nyc_subway.original_encoded_cols_data")

#Step 4:
   # SQL Query to create the `encoded_hour` table with encoded columns
    sql_query_encoded_hour = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.encoded_hour` AS
    SELECT 
        transit_timestamp,
        ridership,
        transfer,
        transit_mode_index,
        station_complex_index,
        borough_index,
        payment_method_index
    FROM `lively-encoder-448916-d5.nyc_subway.original_encoded_cols_data`;
    """

    # Execute the query for encoded_hour
    query_job_encoded_hour = client.query(sql_query_encoded_hour)
    query_job_encoded_hour.result()
    print("Encoded columns have been written to the table: lively-encoder-448916-d5.nyc_subway.encoded_hour")

    # Step 4: Drop original columns and keep only encoded columns in a new table
    #Extract time-based features and compute sine/cosine transformations simultaneously
    sql_query_hour_time_cols = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.hour_time_cols` AS
    SELECT *,
    EXTRACT(HOUR FROM transit_timestamp) AS hour_of_day,
    SIN(2 * ACOS(-1) * EXTRACT(HOUR FROM transit_timestamp) / 24) AS hour_of_day_sin,
    COS(2 * ACOS(-1) * EXTRACT(HOUR FROM transit_timestamp) / 24) AS hour_of_day_cos,
    EXTRACT(DAYOFWEEK FROM transit_timestamp) AS day_of_week,
    SIN(2 * ACOS(-1) * EXTRACT(DAYOFWEEK FROM transit_timestamp) / 7) AS day_of_week_sin,
    COS(2 * ACOS(-1) * EXTRACT(DAYOFWEEK FROM transit_timestamp) / 7) AS day_of_week_cos,
    CEIL(EXTRACT(DAY FROM transit_timestamp) / 7) AS week_of_month,
    SIN(2 * ACOS(-1) * CEIL(EXTRACT(DAY FROM transit_timestamp) / 7) / 5) AS week_of_month_sin,
    COS(2 * ACOS(-1) * CEIL(EXTRACT(DAY FROM transit_timestamp) / 7) / 5) AS week_of_month_cos
    FROM `lively-encoder-448916-d5.nyc_subway.encoded_hour`;
    """

    # Execute the query for hour_time_cols
    query_job_hour_time_cols = client.query(sql_query_hour_time_cols)
    query_job_hour_time_cols.result()
    print("Time-based features have been written to the table: lively-encoder-448916-d5.nyc_subway.hour_time_cols")


    # Step 5: Compute lags and filter rows with NULL lags
    sql_query_lags = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.hour_lags` AS
    WITH lagged_data AS (
        SELECT
            *,
            LAG(ridership, 1) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_1,
            LAG(ridership, 2) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_2,
            LAG(ridership, 3) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_3,
            LAG(ridership, 4) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_4,
            LAG(ridership, 5) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_5,
            LAG(ridership, 6) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_6,
            LAG(ridership, 7) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_7,
            LAG(ridership, 8) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_8,
            LAG(ridership, 9) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_9,
            LAG(ridership, 10) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_10,
            LAG(ridership, 11) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_11,
            LAG(ridership, 12) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_12,
            LAG(ridership, 13) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_13,
            LAG(ridership, 14) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_14,
            LAG(ridership, 15) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_15,
            LAG(ridership, 16) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_16,
            LAG(ridership, 17) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_17,
            LAG(ridership, 18) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_18,
            LAG(ridership, 19) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_19,
            LAG(ridership, 20) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_20,
            LAG(ridership, 21) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_21,
            LAG(ridership, 22) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_22,
            LAG(ridership, 23) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_23,
            LAG(ridership, 24) OVER (
                PARTITION BY transit_mode_index, station_complex_index, borough_index, payment_method_index 
                ORDER BY transit_timestamp
            ) AS ridership_lag_24
        FROM `lively-encoder-448916-d5.nyc_subway.hour_time_cols`
    )
    SELECT *
    FROM lagged_data
    WHERE 
        -- Drop rows where any of the lag columns is NULL
        ARRAY_LENGTH(REGEXP_EXTRACT_ALL(TO_JSON_STRING(lagged_data), r'"ridership_lag_\d+":null')) = 0
    """

    # Execute the query for lags
    query_job_lags = client.query(sql_query_lags)
    query_job_lags.result()
    print("Data has been written to the table: lively-encoder-448916-d5.nyc_subway.hour_lags")

    # Step 6: Compute moving averages and write to a new table
    sql_query_moving_avg = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.hour_model` AS
    WITH moving_avg_data AS (
        SELECT 
            *,
            -- 7-day moving average
            AVG(ridership) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS ridership_7d_mv,
            AVG(hour_of_day) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS hour_of_day_7d_mv,
            AVG(day_of_week) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS day_of_week_7d_mv,
            AVG(week_of_month) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS week_of_month_7d_mv,
            AVG(hour_of_day_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS hour_of_day_sin_7d_mv,
            AVG(hour_of_day_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS hour_of_day_cos_7d_mv,
            AVG(day_of_week_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS day_of_week_sin_7d_mv,
            AVG(day_of_week_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS day_of_week_cos_7d_mv,
            AVG(week_of_month_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS week_of_month_sin_7d_mv,
            AVG(week_of_month_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS week_of_month_cos_7d_mv,
            -- 30-day moving average
            AVG(ridership) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS ridership_30d_mv,
            AVG(hour_of_day) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS hour_of_day_30d_mv,
            AVG(day_of_week) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS day_of_week_30d_mv,
            AVG(week_of_month) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS week_of_month_30d_mv,
            AVG(hour_of_day_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS hour_of_day_sin_30d_mv,
            AVG(hour_of_day_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS hour_of_day_cos_30d_mv,
            AVG(day_of_week_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS day_of_week_sin_30d_mv,
            AVG(day_of_week_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS day_of_week_cos_30d_mv,
            AVG(week_of_month_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS week_of_month_sin_30d_mv,
            AVG(week_of_month_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY UNIX_SECONDS(transit_timestamp)
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS week_of_month_cos_30d_mv
        FROM `lively-encoder-448916-d5.nyc_subway.hour_lags`
    )
    -- Drop rows where any moving average column is NULL
    SELECT *
    FROM moving_avg_data
    WHERE ridership_7d_mv IS NOT NULL
      AND hour_of_day_7d_mv IS NOT NULL
      AND day_of_week_7d_mv IS NOT NULL
      AND week_of_month_7d_mv IS NOT NULL
      AND hour_of_day_sin_7d_mv IS NOT NULL
      AND hour_of_day_cos_7d_mv IS NOT NULL
      AND day_of_week_sin_7d_mv IS NOT NULL
      AND day_of_week_cos_7d_mv IS NOT NULL
      AND week_of_month_sin_7d_mv IS NOT NULL
      AND week_of_month_cos_7d_mv IS NOT NULL
      AND ridership_30d_mv IS NOT NULL
      AND hour_of_day_30d_mv IS NOT NULL
      AND day_of_week_30d_mv IS NOT NULL
      AND week_of_month_30d_mv IS NOT NULL
      AND hour_of_day_sin_30d_mv IS NOT NULL
      AND hour_of_day_cos_30d_mv IS NOT NULL
      AND day_of_week_sin_30d_mv IS NOT NULL
      AND day_of_week_cos_30d_mv IS NOT NULL
      AND week_of_month_sin_30d_mv IS NOT NULL
      AND week_of_month_cos_30d_mv IS NOT NULL
    """

    # Execute the query for moving averages
    query_job_moving_avg = client.query(sql_query_moving_avg)
    query_job_moving_avg.result()
    print("Data has been written to the table: lively-encoder-448916-d5.nyc_subway.hour_model")


if __name__ == "__main__":
    run_bigquery_sql()