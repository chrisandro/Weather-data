USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
CREATE DATABASE weather;
USE DATABASE weather;

-- Create stages
CREATE OR REPLACE SCHEMA landing_zone;
CREATE OR REPLACE SCHEMA curated_zone;
CREATE OR REPLACE SCHEMA consumption_zone;

--------------------------------------------------- LANDING_ZONE CONFIGURATION ---------------------------------------------------

-- Create JSON file format for landing_zone schema
USE SCHEMA landing_zone;

CREATE OR REPLACE FILE FORMAT json_format
    type = 'JSON'
    comment = 'JSON file format weather data
    ';

-- Create an internal stage in the landing_stage schema for storing itinital JSONs
CREATE OR REPLACE STAGE weather_int_stg
    file_format = json_format
    comment = "Milano's weather conditions JSONs every hour";

-- Create landing_json table, where we'll store the json data
CREATE OR REPLACE TRANSIENT TABLE landing_json (
    weather_data VARIANT
    ) comment = 'Landing table for weather data';


--------------------------------------------------- CURATED_ZONE CONFIGURATION ---------------------------------------------------

USE SCHEMA curated_zone;

-- We drop features  like: internal_parameters
CREATE OR REPLACE TRANSIENT TABLE curated_city (
    city_id INT NOT NULL UNIQUE,
    name STRING,
    timezone INT,
    country STRING,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (city_id)
) comment = 'Table with details of each city';

-- We drop features  like: internal_parameters, pressure, sunrise/sunset timestamps, icon, etc.
CREATE OR REPLACE TRANSIENT TABLE curated_weather (
    weather_id INT AUTOINCREMENT,
    city_id INT,
    main STRING,
    description STRING,
    temperature FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure FLOAT,
    humidity FLOAT,
    visibility INT,
    wind_speed FLOAT,
    wind_direction STRING,
    cloudiness INT,
    rain_1h FLOAT,
    rain_3h FLOAT,
    snow_1h FLOAT,
    snow_3h FLOAT,
    dt TIMESTAMP_NTZ,
    PRIMARY KEY (weather_id),
    FOREIGN KEY (city_id) REFERENCES curated_city(city_id)
) comment = 'Table with details of each city';


--------------------------------------------------- CONSUMPTION_ZONE CONFIGURATION ---------------------------------------------------

USE SCHEMA consumption_zone;

-- Create consumption_city table
CREATE OR REPLACE TABLE consumption_city (
    city_id INT NOT NULL UNIQUE,
    name VARCHAR(50),
    timezone VARCHAR(50),
    country VARCHAR(3),
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (city_id)
) comment = 'Table with details of each city';


-- Create distinct_weather_conditions table
CREATE OR REPLACE TABLE distinct_weather_conditions (
    distinct_conditions_count INT,
    distinct_conditions ARRAY
) comment = 'How many distinct weather conditions were observed (rain/snow/clear/…) in a certain period';



-- Create weather_conditions_rank_per_city table
CREATE OR REPLACE TABLE weather_conditions_rank_per_city (
    city_id INT NOT NULL,
    city VARCHAR(50),
    weather_condition VARCHAR(50), 
    frequency INT,
    FOREIGN KEY (city_id) REFERENCES consumption_city(city_id)
) comment = 'Rank the most common weather conditions in a certain period of time per city';


-- Create avg_temp_per_city table
CREATE OR REPLACE TABLE avg_temp_per_city (
    city_id INT NOT NULL UNIQUE,
    city VARCHAR(50),
    avg_temperature FLOAT,
    PRIMARY KEY (city_id),
    FOREIGN KEY (city_id) REFERENCES consumption_city(city_id)
) comment = 'What are the temperature averages observed in a certain period per city';


-- Create max_abs_temp table
CREATE OR REPLACE TABLE max_abs_temp (
    city_id INT NOT NULL UNIQUE,
    city VARCHAR(50),
    max_abs_temperature FLOAT,
    FOREIGN KEY (city_id) REFERENCES consumption_city(city_id)
) comment = 'What city had the highest absolute temperature in a certain period of time';


-- Create max_daily_temp_variation table
CREATE OR REPLACE TABLE max_daily_temp_variation (
    city_id INT NOT NULL UNIQUE,
    city VARCHAR(50),
    "date" DATE,
    temperature_variation FLOAT,
    FOREIGN KEY (city_id) REFERENCES consumption_city(city_id)
) comment = 'Which city had the highest daily temperature variation in a certain period of time';


-- Create city_with_strongest_wind table
CREATE OR REPLACE TABLE city_with_strongest_wind (
    city_id INT NOT NULL UNIQUE,
    city VARCHAR(50),
    max_wind_speed FLOAT,
    FOREIGN KEY (city_id) REFERENCES consumption_city(city_id)
) comment = 'What city had the strongest wind in a certain period of time';

