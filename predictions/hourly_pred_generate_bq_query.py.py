from google.cloud import bigquery

def create_hourly_tables():
    # Initialize BigQuery client
    client = bigquery.Client()

    # Step 1: Create `hourly_pred_lags` table
    query_step_1 = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.hourly_pred_lags`
    AS
    WITH ridership_lags AS (
        SELECT
            (SELECT TIMESTAMP_ADD(MAX(transit_timestamp), INTERVAL 1 HOUR)
             FROM `lively-encoder-448916-d5.nyc_subway.hour_model`) AS transit_timestamp,
            transit_mode_index, station_complex_index, borough_index, payment_method_index,
            ridership as ridership_lag_1,
            ridership_lag_1 as ridership_lag_2,
            ridership_lag_2 as ridership_lag_3,
            ridership_lag_3 as ridership_lag_4,
            ridership_lag_4 as ridership_lag_5,
            ridership_lag_5 as ridership_lag_6,
            ridership_lag_6 as ridership_lag_7,
            ridership_lag_7 as ridership_lag_8,
            ridership_lag_8 as ridership_lag_9,
            ridership_lag_9 as ridership_lag_10,
            ridership_lag_10 as ridership_lag_11,
            ridership_lag_11 as ridership_lag_12,
            ridership_lag_12 as ridership_lag_13,
            ridership_lag_13 as ridership_lag_14,
            ridership_lag_14 as ridership_lag_15,
            ridership_lag_15 as ridership_lag_16,
            ridership_lag_16 as ridership_lag_17,
            ridership_lag_17 as ridership_lag_18,
            ridership_lag_18 as ridership_lag_19,
            ridership_lag_19 as ridership_lag_20,
            ridership_lag_20 as ridership_lag_21,
            ridership_lag_21 as ridership_lag_22,
            ridership_lag_22 as ridership_lag_23,
            ridership_lag_23 as ridership_lag_24     
        FROM `lively-encoder-448916-d5.nyc_subway.hour_model`
        WHERE transit_timestamp = (SELECT MAX(transit_timestamp)
                                   FROM `lively-encoder-448916-d5.nyc_subway.hour_model`)
    )
    SELECT * FROM ridership_lags;
    """
    client.query(query_step_1).result()
    print("Step 1: Created `hourly_pred_lags` table.")

    # Step 2: Create `hourly_pred_moving_avg` table
    query_step_2 = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.hourly_pred_moving_avg`
    AS
    WITH ridership_moving_avg AS (
        SELECT 
            transit_timestamp,
            transit_mode_index,
            station_complex_index,
            borough_index,
            payment_method_index,
            -- 7-day moving average
            AVG(ridership) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS ridership_7d_mv,
            AVG(hour_of_day) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS hour_of_day_7d_mv,
            AVG(day_of_week) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_7d_mv,
            AVG(week_of_month) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_7d_mv,
            AVG(hour_of_day_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS hour_of_day_sin_7d_mv,
             AVG(hour_of_day_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS hour_of_day_cos_7d_mv,
            AVG(day_of_week_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_sin_7d_mv,
            AVG(day_of_week_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_cos_7d_mv,
            AVG(week_of_month_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_sin_7d_mv,
            AVG(week_of_month_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_cos_7d_mv,
            -- 30-day moving average
            AVG(ridership) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS ridership_30d_mv,
            AVG(hour_of_day) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS hour_of_day_30d_mv,
            AVG(day_of_week) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_30d_mv,
            AVG(week_of_month) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_30d_mv,
            AVG(hour_of_day_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS hour_of_day_sin_30d_mv,
            AVG(hour_of_day_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS hour_of_day_cos_30d_mv,
            AVG(day_of_week_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_sin_30d_mv,
            AVG(day_of_week_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_cos_30d_mv,
            AVG(week_of_month_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_sin_30d_mv,
            AVG(week_of_month_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_timestamp
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_cos_30d_mv
        FROM `lively-encoder-448916-d5.nyc_subway.hour_model`
        WHERE transit_timestamp >=  (SELECT TIMESTAMP_SUB(MAX(transit_timestamp), INTERVAL 29 HOUR) 
                                     FROM `lively-encoder-448916-d5.nyc_subway.hour_model`) 
          AND transit_timestamp <= (SELECT MAX(transit_timestamp) 
                                    FROM `lively-encoder-448916-d5.nyc_subway.hour_model`)
    )
    SELECT
        (SELECT TIMESTAMP_ADD(MAX(transit_timestamp), INTERVAL 1 HOUR)
         FROM `lively-encoder-448916-d5.nyc_subway.hour_model`) AS next_transit_timestamp,
        *
    FROM
        ridership_moving_avg
    WHERE transit_timestamp = (SELECT MAX(transit_timestamp) FROM `lively-encoder-448916-d5.nyc_subway.hour_model`)
    ORDER BY transit_timestamp DESC;
    """
    client.query(query_step_2).result()
    print("Step 2: Created `hourly_pred_moving_avg` table.")

    # Step 3: Create `hourly_future_pred_input` table
    query_step_3 = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.hourly_future_pred_input`
    AS (
      SELECT
        l.transit_mode_index,
        l.station_complex_index,
        l.borough_index,
        l.payment_method_index,
        EXTRACT(HOUR FROM l.transit_timestamp) AS hour_of_day,
        SIN(2 * ACOS(-1) * EXTRACT(HOUR FROM l.transit_timestamp) / 24) AS hour_of_day_sin,
        COS(2 * ACOS(-1) * EXTRACT(HOUR FROM l.transit_timestamp) / 24) AS hour_of_day_cos,
        EXTRACT(DAYOFWEEK FROM l.transit_timestamp) AS day_of_week,
        SIN(2 * ACOS(-1) * EXTRACT(DAYOFWEEK FROM l.transit_timestamp) / 7) AS day_of_week_sin,
        COS(2 * ACOS(-1) * EXTRACT(DAYOFWEEK FROM  l.transit_timestamp) / 7) AS day_of_week_cos,
        CEIL(EXTRACT(DAY FROM  l.transit_timestamp) / 7) AS week_of_month,
        SIN(2 * ACOS(-1) * CEIL(EXTRACT(DAY FROM  l.transit_timestamp) / 7) / 5) AS week_of_month_sin,
        COS(2 * ACOS(-1) * CEIL(EXTRACT(DAY FROM  l.transit_timestamp) / 7) / 5) AS week_of_month_cos,
        l.ridership_lag_1, l.ridership_lag_2, l.ridership_lag_3, l.ridership_lag_4, l.ridership_lag_5,
        l.ridership_lag_6, l.ridership_lag_7, l.ridership_lag_8, l.ridership_lag_9, l.ridership_lag_10,
        l.ridership_lag_11, l.ridership_lag_12, l.ridership_lag_13, l.ridership_lag_14, l.ridership_lag_15,
        l.ridership_lag_16, l.ridership_lag_17, l.ridership_lag_18, l.ridership_lag_19, l.ridership_lag_20,
        l.ridership_lag_21, l.ridership_lag_22, l.ridership_lag_23, l.ridership_lag_24,
        mv.ridership_7d_mv,
        mv.hour_of_day_7d_mv, mv.day_of_week_7d_mv, mv.week_of_month_7d_mv,
        mv.hour_of_day_sin_7d_mv, mv.hour_of_day_cos_7d_mv,
        mv.day_of_week_sin_7d_mv, mv.day_of_week_cos_7d_mv,
        mv.week_of_month_sin_7d_mv, mv.week_of_month_cos_7d_mv,
        mv.ridership_30d_mv,
        mv.hour_of_day_30d_mv, mv.day_of_week_30d_mv, mv.week_of_month_30d_mv,
        mv.hour_of_day_sin_30d_mv, mv.hour_of_day_cos_30d_mv,
        mv.day_of_week_sin_30d_mv, mv.day_of_week_cos_30d_mv,
        mv.week_of_month_sin_30d_mv, mv.week_of_month_cos_30d_mv
        FROM `lively-encoder-448916-d5.nyc_subway.hourly_pred_lags` l
        INNER JOIN `lively-encoder-448916-d5.nyc_subway.hourly_pred_moving_avg` mv
        ON l.transit_mode_index = mv.transit_mode_index
        AND l.station_complex_index = mv.station_complex_index
        AND l.borough_index = mv.borough_index
        AND l.payment_method_index = mv.payment_method_index
    );
    """
    client.query(query_step_3).result()
    print("Step 3: Created `hourly_future_pred_input` table.")

if __name__ == "__main__":
    create_hourly_tables()