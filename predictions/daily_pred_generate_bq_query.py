from google.cloud import bigquery

def create_tables():
    # Initialize BigQuery client
    client = bigquery.Client()

    # Step 1: Create `daily_pred_lags` table
    query_step_1 = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.daily_pred_lags`
    AS
    WITH ridership_lags AS (
        SELECT
            (SELECT MAX(transit_date)+1 FROM `lively-encoder-448916-d5.nyc_subway.date_model`) AS transit_date,
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
            ridership_lag_23 as ridership_lag_24,
            ridership_lag_24 as ridership_lag_25,
            ridership_lag_25 as ridership_lag_26,
            ridership_lag_26 as ridership_lag_27,
            ridership_lag_27 as ridership_lag_28,
            ridership_lag_28 as ridership_lag_29,
            ridership_lag_29 as ridership_lag_30
        FROM `lively-encoder-448916-d5.nyc_subway.date_model`
        WHERE transit_date = (SELECT MAX(transit_date) FROM `lively-encoder-448916-d5.nyc_subway.date_model`)
        LIMIT 5000
    )
    SELECT * FROM ridership_lags;
    """
    client.query(query_step_1).result()
    print("Step 1: Created `daily_pred_lags` table.")

    # Step 2: Create `daily_pred_moving_avg` table
    query_step_2 = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.daily_pred_moving_avg`
    AS
    WITH ridership_moving_avg AS (
        SELECT 
            transit_date,
            transit_mode_index,
            station_complex_index,
            borough_index,
            payment_method_index,
            -- 7-day moving average
            AVG(ridership) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS ridership_7d_mv,
            AVG(day_of_week) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_7d_mv,
            AVG(week_of_month) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_7d_mv,
            AVG(day_of_week_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_sin_7d_mv,
            AVG(day_of_week_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_cos_7d_mv,
            AVG(week_of_month_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_sin_7d_mv,
            AVG(week_of_month_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 6 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_cos_7d_mv,
            -- 30-day moving average
            AVG(ridership) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS ridership_30d_mv,
            AVG(day_of_week) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_30d_mv,
            AVG(week_of_month) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_30d_mv,
            AVG(day_of_week_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_sin_30d_mv,
            AVG(day_of_week_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS day_of_week_cos_30d_mv,
            AVG(week_of_month_sin) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_sin_30d_mv,
            AVG(week_of_month_cos) OVER (
                PARTITION BY station_complex_index, transit_mode_index, borough_index, payment_method_index
                ORDER BY transit_date
                ROWS BETWEEN 29 PRECEDING AND 0 PRECEDING
            ) AS week_of_month_cos_30d_mv
        FROM `lively-encoder-448916-d5.nyc_subway.date_model`
        WHERE transit_date BETWEEN 
            (SELECT MAX(transit_date) - 29 FROM `lively-encoder-448916-d5.nyc_subway.date_model`) 
            AND 
            (SELECT MAX(transit_date) FROM `lively-encoder-448916-d5.nyc_subway.date_model`)
    )
    SELECT
        transit_date + 1 AS next_transit_date,
        *
    FROM
        ridership_moving_avg
    WHERE transit_date = (SELECT MAX(transit_date) FROM `lively-encoder-448916-d5.nyc_subway.date_model`)
    ORDER BY transit_date DESC
    LIMIT 5000;
    """
    client.query(query_step_2).result()
    print("Step 2: Created `daily_pred_moving_avg` table.")

    # Step 3: Create `daily_future_pred_input` table
    query_step_3 = """
    CREATE OR REPLACE TABLE `lively-encoder-448916-d5.nyc_subway.daily_future_pred_input`
    AS (
        SELECT
            l.transit_mode_index,
            l.station_complex_index,
            l.borough_index,
            l.payment_method_index,
            EXTRACT(DAYOFWEEK FROM l.transit_date) AS day_of_week,
            SIN(2 * ACOS(-1) * EXTRACT(DAYOFWEEK FROM l.transit_date) / 7) AS day_of_week_sin,
            COS(2 * ACOS(-1) * EXTRACT(DAYOFWEEK FROM l.transit_date) / 7) AS day_of_week_cos,
            CEIL(EXTRACT(DAY FROM l.transit_date) / 7) AS week_of_month,
            SIN(2 * ACOS(-1) * CEIL(EXTRACT(DAY FROM l.transit_date) / 7) / 5) AS week_of_month_sin,
            COS(2 * ACOS(-1) * CEIL(EXTRACT(DAY FROM l.transit_date) / 7) / 5) AS week_of_month_cos,
            l.ridership_lag_1, l.ridership_lag_2, l.ridership_lag_3, l.ridership_lag_4, l.ridership_lag_5,
            l.ridership_lag_6, l.ridership_lag_7, l.ridership_lag_8, l.ridership_lag_9, l.ridership_lag_10,
            l.ridership_lag_11, l.ridership_lag_12, l.ridership_lag_13, l.ridership_lag_14, l.ridership_lag_15,
            l.ridership_lag_16, l.ridership_lag_17, l.ridership_lag_18, l.ridership_lag_19, l.ridership_lag_20,
            l.ridership_lag_21, l.ridership_lag_22, l.ridership_lag_23, l.ridership_lag_24, l.ridership_lag_25,
            l.ridership_lag_26, l.ridership_lag_27, l.ridership_lag_28, l.ridership_lag_29, l.ridership_lag_30,
            mv.ridership_7d_mv,
            mv.day_of_week_7d_mv, mv.week_of_month_7d_mv,
            mv.day_of_week_sin_7d_mv, mv.day_of_week_cos_7d_mv,
            mv.week_of_month_sin_7d_mv, mv.week_of_month_cos_7d_mv,
            mv.ridership_30d_mv,
            mv.day_of_week_30d_mv, mv.week_of_month_30d_mv,
            mv.day_of_week_sin_30d_mv, mv.day_of_week_cos_30d_mv,
            mv.week_of_month_sin_30d_mv, mv.week_of_month_cos_30d_mv
        FROM `lively-encoder-448916-d5.nyc_subway.daily_pred_lags` l
        INNER JOIN `lively-encoder-448916-d5.nyc_subway.daily_pred_moving_avg` mv
        ON l.transit_mode_index = mv.transit_mode_index
        AND l.station_complex_index = mv.station_complex_index
        AND l.borough_index = mv.borough_index
        AND l.payment_method_index = mv.payment_method_index
    );
    """
    client.query(query_step_3).result()
    print("Step 3: Created `daily_future_pred_input` table.")

if __name__ == "__main__":
    create_tables()