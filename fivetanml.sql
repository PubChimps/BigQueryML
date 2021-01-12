CREATE OR REPLACE MODEL nick.pg_public_stripe_arima_model
OPTIONS
 (model_type = 'ARIMA',
  time_series_timestamp_col = 'parsed_date',
  time_series_data_col = 'mar',
  auto_arima = TRUE,
  data_frequency = 'AUTO_FREQUENCY'
) AS
SELECT
  TIMESTAMP(DATE(measured)) AS parsed_date, 
  sum(daily_rows) as mar  
FROM
  pg_public.active_volume_by_integration 
WHERE
  integration = 'stripe' and DATE(measured) > '2020-02-28'
GROUP BY
  parsed_date 


SELECT t0.forecast_value, t0.history_value, t0.prediction_interval_lower_bound, t0.prediction_interval_upper_bound, t0.timestamp FROM (SELECT
  history_timestamp AS timestamp,
  history_value,
  NULL AS forecast_value,
  NULL AS prediction_interval_lower_bound,
  NULL AS prediction_interval_upper_bound
FROM
  (
    SELECT
      TIMESTAMP(DATE(measured)) AS history_timestamp, 
      sum(daily_rows) as history_value  
    FROM
      pg_public.active_volume_by_integration 
    WHERE
      integration = 'stripe' and DATE(measured) > '2020-02-28' and DATE(measured) < '2021-01-12'
    GROUP BY
      history_timestamp 
    ORDER BY history_timestamp ASC
  )
UNION ALL
SELECT
  forecast_timestamp AS timestamp,
  NULL AS history_value,
  forecast_value,
  prediction_interval_lower_bound,
  prediction_interval_upper_bound
FROM
  ML.FORECAST(MODEL nick.pg_public_stripe_arima_model,
              STRUCT(50 AS horizon, 0.5 AS confidence_level))) AS t0; 
