CREATE OR REPLACE VIEW `uk-weather-dashboard.weather_mart.latest_weather_by_city` AS
WITH ranked AS (
  SELECT
    city,
    source_api,
    observed_dt,
    temperature_c,
    windspeed_kph,
    wind_direction_deg,
    condition_code,
    condition_text,
    humidity,
    _sdc_extracted_at,
    _sdc_received_at,
    ROW_NUMBER() OVER (
      PARTITION BY city, source_api
      ORDER BY observed_dt DESC, _sdc_received_at DESC
    ) AS rn
  FROM `uk-weather-dashboard.weather_mart.current_weather_uk`
)
SELECT
  city,
  source_api,
  observed_dt,
  temperature_c,
  windspeed_kph,
  wind_direction_deg,
  condition_code,
  condition_text,
  humidity,
  _sdc_extracted_at,
  _sdc_received_at
FROM ranked
WHERE rn = 1;