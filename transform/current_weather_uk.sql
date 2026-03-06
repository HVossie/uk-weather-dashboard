CREATE SCHEMA IF NOT EXISTS `uk-weather-dashboard.weather_mart`;

CREATE OR REPLACE VIEW `uk-weather-dashboard.weather_mart.current_weather_uk` AS

/* ---------------- OPEN METEO ---------------- */

SELECT
  'London' AS city,
  'open-meteo' AS source_api,
  PARSE_DATETIME('%Y-%m-%dT%H:%M', JSON_VALUE(data, '$.time')) AS observed_dt,
  CAST(JSON_VALUE(data, '$.temperature') AS FLOAT64) AS temperature_c,
  CAST(JSON_VALUE(data, '$.windspeed') AS FLOAT64) AS windspeed_kph,
  CAST(JSON_VALUE(data, '$.winddirection') AS INT64) AS wind_direction_deg,
  CAST(JSON_VALUE(data, '$.weathercode') AS STRING) AS condition_code,
  NULL AS condition_text,
  NULL AS humidity,
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.open_meteo_london_current`

UNION ALL

SELECT
  'Manchester',
  'open-meteo',
  PARSE_DATETIME('%Y-%m-%dT%H:%M', JSON_VALUE(data, '$.time')),
  CAST(JSON_VALUE(data, '$.temperature') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.windspeed') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.winddirection') AS INT64),
  CAST(JSON_VALUE(data, '$.weathercode') AS STRING),
  NULL,
  NULL,
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.open_meteo_manchester_current`

UNION ALL

SELECT
  'Glasgow',
  'open-meteo',
  PARSE_DATETIME('%Y-%m-%dT%H:%M', JSON_VALUE(data, '$.time')),
  CAST(JSON_VALUE(data, '$.temperature') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.windspeed') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.winddirection') AS INT64),
  CAST(JSON_VALUE(data, '$.weathercode') AS STRING),
  NULL,
  NULL,
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.open_meteo_glasgow_current`


/* ---------------- OPENWEATHER ---------------- */

UNION ALL

SELECT
  JSON_VALUE(data, '$.name') AS city,
  'openweather' AS source_api,
  DATETIME(TIMESTAMP_SECONDS(CAST(JSON_VALUE(data, '$.dt') AS INT64)), "Europe/London") AS observed_dt,
  CAST(JSON_VALUE(data, '$.main_temp') AS FLOAT64) AS temperature_c,
  CAST(JSON_VALUE(data, '$.wind_speed') AS FLOAT64) AS windspeed_kph,
  CAST(JSON_VALUE(data, '$.wind_deg') AS INT64) AS wind_direction_deg,
  JSON_VALUE(data, '$.weather[0].id') AS condition_code,
  JSON_VALUE(data, '$.weather[0].description') AS condition_text,
  CAST(JSON_VALUE(data, '$.main_humidity') AS INT64) AS humidity,
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.openweather_london_current`

UNION ALL

SELECT
  JSON_VALUE(data, '$.name'),
  'openweather',
  DATETIME(TIMESTAMP_SECONDS(CAST(JSON_VALUE(data, '$.dt') AS INT64)), "Europe/London"),
  CAST(JSON_VALUE(data, '$.main_temp') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.wind_speed') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.wind_deg') AS INT64),
  JSON_VALUE(data, '$.weather[0].id'),
  JSON_VALUE(data, '$.weather[0].description'),
  CAST(JSON_VALUE(data, '$.main_humidity') AS INT64),
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.openweather_manchester_current`

UNION ALL

SELECT
  JSON_VALUE(data, '$.name'),
  'openweather',
  DATETIME(TIMESTAMP_SECONDS(CAST(JSON_VALUE(data, '$.dt') AS INT64)), "Europe/London"),
  CAST(JSON_VALUE(data, '$.main_temp') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.wind_speed') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.wind_deg') AS INT64),
  JSON_VALUE(data, '$.weather[0].id'),
  JSON_VALUE(data, '$.weather[0].description'),
  CAST(JSON_VALUE(data, '$.main_humidity') AS INT64),
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.openweather_glasgow_current`


/* ---------------- WEATHERAPI ---------------- */

UNION ALL

SELECT
  JSON_VALUE(data, '$.location_name') AS city,
  'weatherapi' AS source_api,
  PARSE_DATETIME('%Y-%m-%d %H:%M', JSON_VALUE(data, '$.current_last_updated')) AS observed_dt,
  CAST(JSON_VALUE(data, '$.current_temp_c') AS FLOAT64) AS temperature_c,
  CAST(JSON_VALUE(data, '$.current_wind_kph') AS FLOAT64) AS windspeed_kph,
  CAST(JSON_VALUE(data, '$.current_wind_degree') AS INT64) AS wind_direction_deg,
  CAST(JSON_VALUE(data, '$.current_condition_code') AS STRING) AS condition_code,
  JSON_VALUE(data, '$.current_condition_text') AS condition_text,
  CAST(JSON_VALUE(data, '$.current_humidity') AS INT64) AS humidity,
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.weatherapi_london_current`

UNION ALL

SELECT
  JSON_VALUE(data, '$.location_name'),
  'weatherapi',
  PARSE_DATETIME('%Y-%m-%d %H:%M', JSON_VALUE(data, '$.current_last_updated')),
  CAST(JSON_VALUE(data, '$.current_temp_c') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.current_wind_kph') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.current_wind_degree') AS INT64),
  CAST(JSON_VALUE(data, '$.current_condition_code') AS STRING),
  JSON_VALUE(data, '$.current_condition_text'),
  CAST(JSON_VALUE(data, '$.current_humidity') AS INT64),
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.weatherapi_manchester_current`

UNION ALL

SELECT
  JSON_VALUE(data, '$.location_name'),
  'weatherapi',
  PARSE_DATETIME('%Y-%m-%d %H:%M', JSON_VALUE(data, '$.current_last_updated')),
  CAST(JSON_VALUE(data, '$.current_temp_c') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.current_wind_kph') AS FLOAT64),
  CAST(JSON_VALUE(data, '$.current_wind_degree') AS INT64),
  CAST(JSON_VALUE(data, '$.current_condition_code') AS STRING),
  JSON_VALUE(data, '$.current_condition_text'),
  CAST(JSON_VALUE(data, '$.current_humidity') AS INT64),
  _sdc_extracted_at,
  _sdc_received_at
FROM `uk-weather-dashboard.weather_raw.weatherapi_glasgow_current`;