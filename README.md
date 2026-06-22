# UK Weather Data Pipeline

End-to-end data pipeline that collects current weather data from multiple APIs, loads it into Google BigQuery, and visualizes the results in Looker Studio.

This project was built to practice ELT pipeline design, cloud data warehousing, SQL transformation, and dashboard analytics in a small but complete analytics workflow.

## Live Dashboard

[View Dashboard](https://lookerstudio.google.com/s/vhWFYjagW2E)

## Features

- Extract weather data from multiple APIs
- Load raw records into Google BigQuery
- Transform source data into unified reporting views
- Compare weather readings across providers and cities
- Visualize results in Looker Studio
- Run ingestion with a Meltano-managed pipeline
- Schedule automated pipeline runs with GitHub Actions

## Tech Stack

- Python 3
- Meltano
- Singer taps and target
- Google BigQuery
- Looker Studio
- GitHub Actions

## Data Sources

The pipeline collects current weather data for:

- London
- Manchester
- Glasgow

Weather providers used:

### Open-Meteo
- Temperature
- Wind speed
- Wind direction
- Weather code
- Observation time

### OpenWeather
- Temperature
- Humidity
- Pressure
- Wind speed
- Weather description

### WeatherAPI
- Temperature
- Feels-like temperature
- Wind speed
- Visibility
- Humidity
- Condition text

Using multiple providers allows the dashboard to compare readings across APIs for the same cities.

## How to Run

1. Clone the repository

```bash
git clone https://github.com/HVossie/uk-weather-dashboard.git
```

2. Navigate into the project

```bash
cd uk-weather-dashboard
```

3. Install dependencies

```bash
pip install meltano
```

4. Install Meltano plugins

```bash
meltano install
```

5. Create a `.env` file with your API keys

```env
OPENWEATHER_API_KEY=your_api_key_here
WEATHERAPI_KEY=your_api_key_here
```

6. Authenticate with Google Cloud for BigQuery access

7. Run the pipelines

```bash
meltano run tap-rest-api-msdk target-bigquery
meltano run tap-openweather target-bigquery
meltano run tap-weatherapi target-bigquery
```

## Project Structure

```text
uk-weather-dashboard/
|-- .github/
|   `-- workflows/
|       `-- daily-weather-pipeline.yml   # Scheduled GitHub Actions pipeline
|-- analyze/                             # Analysis placeholders and queries
|-- extract/                             # Extraction-related workspace
|-- load/                                # Loading-related workspace
|-- notebook/                            # Exploration notebooks
|-- orchestrate/                         # Orchestration-related workspace
|-- output/                              # Generated outputs
|-- screenshots/                         # Dashboard images for the README
|-- transform/                           # BigQuery SQL transformation views
|   |-- current_weather_uk.sql
|   |-- latest_weather_by_city.sql
|   `-- open_meteo_current_uk.sql
|-- meltano.yml                          # Meltano extractor and loader config
|-- requirements.txt
|-- README.md
`-- .gitignore
```

## What I Learned

- Building ELT pipelines with Meltano
- Integrating multiple third-party APIs into one workflow
- Loading raw source data into Google BigQuery
- Writing SQL views for a unified analytics layer
- Designing dashboards in Looker Studio
- Automating scheduled data runs with GitHub Actions

## Possible Improvements

- Add historical weather tracking over time
- Add data quality checks and validation steps
- Expand to more UK cities or additional weather APIs
- Add tests around SQL transformations or pipeline outputs
- Improve setup automation for local development

## Dashboard Preview

### Overview
![Overview](screenshots/dashboard_overview.png)

### Temperature Comparison by City and API
![Temperature Comparison](screenshots/temperature_comparison.png)

### Raw Weather Data Table
![Weather Table](screenshots/weather_table.png)

## Author

- Hanroux Vos
