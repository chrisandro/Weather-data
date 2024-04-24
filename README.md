# Weather data scraper - OpenWeatherMap API

This is a repository for automatically downloading the current weather conditions of three italian cities: Milan, Bologna, Cagliari. The data comes as responds to scheduled requests that are sent to the [OpenWeatherMap API](https://openweathermap.org/current#builtin) every hour. For obtaining the lontitude and latitude coordinates of each city, necessary for the implementation of city-specific requests, a different request is sent to the same API at the beginning of the process. After download, the data is stored in the path indicated by the user in JSON format. Every time a new JSON file lands in that folder, its content is ingested into a Snowflake (data warehouse) internal stage automatically, using Snowflake's connector with Python (snowflake-connector-python). 

The user may set the following parameters through the params.yaml: 
* *api_key* - subscription key
* *data_folder_path* - folder where the data will be saved
* *Snowflake parameters*
    * password
    * account
    * warehouse
    * database
    * scheme
    * stage

## Execute the code
To start scraping data all you have to do is to clone this repository, and set the parameters *data_folder_path* (where the data will land) and *api_key* (communication with the weather API). Finally, execute the script scraper.py. Python is required to be installed on your machine, along with its necessary libraries listed in the requirements.txt. Data in JSON format will start landing in the defined directory every hour (one JSON per city). After arrival, the data is ingested to the data warehouse automatically (if configuration parameters have been set in the params.yaml).

To test only the downloader execute the script download_only.py

## Docker deployment instructions

The docker has as entrypoint the scraper.py script, which means that whenever the user runs the docker image of the service, the script is executed automatically.

To deploy the image on a new machine use the following command:

```bash
docker build -t weather-scraper -f scraper.Dockerfile .
```

To run the deployed docker image use the following command:

```bash
docker run --rm --name weather-scraper-instance -t weather-scraper
```

## JSON format API response example

```json
{
    "coord": {
        "lon": 11.3426,
        "lat": 44.4938
    },
    "weather": [
        {
            "id": 800,
            "main": "Clear",
            "description": "clear sky",
            "icon": "01d"
        }
    ],
    "base": "stations",
    "main": {
        "temp": 296.14,
        "feels_like": 295.93,
        "temp_min": 295.51,
        "temp_max": 297.08,
        "pressure": 1015,
        "humidity": 55
    },
    "visibility": 10000,
    "wind": {
        "speed": 5.66,
        "deg": 90
    },
    "clouds": {
        "all": 0
    },
    "dt": 1712590807,
    "sys": {
        "type": 2,
        "id": 2000038,
        "country": "IT",
        "sunrise": 1712551338,
        "sunset": 1712598604
    },
    "timezone": 7200,
    "id": 3181928,
    "name": "Bologna",
    "cod": 200
}
```

## Snowflake configuration
For the whole ETL workflow impementation a free trial/personal [Snowflake account](https://signup.snowflake.com/) is required. Once the account is created, the user should fill the account details in the params.yaml, before executing the data download service. 

For the configuration of the database architecture the user may run the SQL code blocks that are documented in the **DATABASE_CONFIG.sql** worksheet. These queries will create the database, the different schemas (look data model) and tables, as well as the file format and warehouse internal stage where the JSON data will land.

The **LOAD&QUERY.sql** worksheet manually ingests into the internal stage, all the history data that are contained in the hist_data folder, and copies them into the landing_json table of the landing_zone schema. Then the data is further split into two tables in the curated_zone, where non-important features are dropped and the data is corrected and cured. Finally the worksheet ends with queries that answer various questions regarding the weather conditions in all three cities (questions posed in the assessment test). For more info regarding the data curing process see ETL_workflow_explanation.pdf.

The code blocks in the scripts **SNOWPIPE.sql**, **STREAMS.sql** and **TASKS.sql** implement the main components of the ETL pipeline (monitoring of INSERT & UPDATES statements in staging tables, rules for passing the data from one table to another, auto-ingestion of new data arriving to the warehouse internal stage).
