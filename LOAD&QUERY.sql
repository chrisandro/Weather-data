-------------------------------------------------- LOAD HISTORICAL DATA --------------------------------------------------
USE DATABASE weather;
USE SCHEMA landing_zone;

-- Use the following code, replacing the {path/to/hist_data} with the actual absolute path of the hist_data folder that contains the initial JSONs
--PUT FILE:///path/to/hist_data/*.json @weather_int_stg/1-hour-scraping auto_compress=false;

COPY INTO landing_json FROM @weather_int_stg;

SELECT * FROM landing_json LIMIT 10;

-------------------------------------------------- INSERT HISTORICAL DATA TO TABLES --------------------------------------------------
USE SCHEMA curated_zone;

-- DATA ISSUES: 
-- city_id not stable (Milan changes between 6542283 and 3173435) -> had to create auto-increment PK for the city ID (another way would be to make all IDs the same)
-- latitude and longitude had to be rounded to one decimal digit to avoid duplications between slight differences in the geo-coordinates
-- wrong timezone numbers (sometimes instead of 3600 we have 7200) -> had to create an IF statement to correct it (another way would be to completely exclude timezone column)
-- other cities are contained in the JSONs (i.e. Sant' Avendrace) -> had to take only the rows with correct city names in the WHERE part of the query

-- Populate curated_city table
INSERT INTO weather.curated_zone.curated_city
SELECT DISTINCT
    weather_data:id::integer AS city_id,
    weather_data:name::string AS name,
    CASE
    WHEN weather_data:timezone::integer <> 3600 THEN 3600
    ELSE weather_data:timezone::integer
    END AS timezone,
    weather_data:sys.country::string AS country,
    ROUND(weather_data:coord.lat::float, 1) AS latitude,
    ROUND(weather_data:coord.lon::float, 1) AS longitude
FROM
    weather.landing_zone.landing_json
WHERE weather_data:name::string = 'Milan' OR weather_data:name::string = 'Bologna' OR weather_data:name::string = 'Cagliari';

SELECT * FROM curated_city;

-- DATA ISSUES: It is possible to meet more than one weather condition for a requested location. We keep only the first weather condition in API respond, which is primary.
-- Populate curated_weather table
INSERT INTO weather.curated_zone.curated_weather (
    city_id, main, description, temperature, feels_like, temp_min, temp_max, pressure, humidity,
    visibility, wind_speed, wind_direction, cloudiness, rain_1h, rain_3h, snow_1h, snow_3h, dt
)
SELECT 
    weather_data:id::integer AS city_id,
    weather_data:weather[0].main::string AS main,
    weather_data:weather[0].description::string AS description,
    weather_data:main.temp::float AS temprerature,
    weather_data:main.feels_like::float AS feels_like,
    weather_data:main.temp_min::float AS temp_min,
    weather_data:main.temp_max::float AS temp_max,
    weather_data:main.pressure::float AS pressure,
    weather_data:main.humidity::float AS humidity,
    weather_data:visibility::integer AS visibility,
    weather_data:wind.speed::float AS wind_speed,
    CASE
    WHEN weather_data:wind.deg::float >= 337.5 OR weather_data:wind.deg::float < 22.5 THEN 'North'
    WHEN weather_data:wind.deg::float >= 22.5 AND weather_data:wind.deg::float < 67.5 THEN 'North-East'
    WHEN weather_data:wind.deg::float >= 67.5 AND weather_data:wind.deg::float < 112.5 THEN 'East'
    WHEN weather_data:wind.deg::float >= 112.5 AND weather_data:wind.deg::float < 157.5 THEN 'South-East'
    WHEN weather_data:wind.deg::float >= 157.5 AND weather_data:wind.deg::float < 202.5 THEN 'South'
    WHEN weather_data:wind.deg::float >= 202.5 AND weather_data:wind.deg::float < 247.5 THEN 'South-West'
    WHEN weather_data:wind.deg::float >= 247.5 AND weather_data:wind.deg::float < 292.5 THEN 'West'
    WHEN weather_data:wind.deg::float >= 292.5 AND weather_data:wind.deg::float < 337.5 THEN 'North-West'
    END AS wind_direction,
    weather_data:clouds.all::float AS cloudiness,
    weather_data:rain."1h"::float AS rain_1h,
    weather_data:rain."3h"::float AS rain_3h,
    weather_data:snow."1h"::float AS snow_1h,
    weather_data:snow."3h"::float AS snow_3h,
    TO_TIMESTAMP(weather_data:dt::integer) AS dt
FROM
    weather.landing_zone.landing_json
WHERE weather_data:name::string = 'Milan' OR weather_data:name::string = 'Bologna' OR weather_data:name::string = 'Cagliari';


SELECT * FROM curated_weather;


-------------------------------------------------- QUERY DATA --------------------------------------------------

-- How many distinct weather conditions were observed (rain/snow/clear/…) in a certain period?
SELECT 
    COUNT(DISTINCT curated_weather.main) AS distinct_conditions_count,
    ARRAY_AGG(DISTINCT curated_weather.main) AS distinct_conditions
FROM 
    curated_weather
WHERE dt >= '2024-03-29' AND dt <= '2024-04-02';




-- How many distinct weather conditions were observed (rain/snow/clear/…) in a certain period per city?
SELECT 
    curated_city.name AS city,
    COUNT(DISTINCT curated_weather.main) AS distinct_conditions_count,
    ARRAY_AGG(DISTINCT curated_weather.main) AS distinct_conditions
FROM
    curated_weather
JOIN
    curated_city ON  curated_city.city_id = curated_weather.city_id
WHERE curated_weather.dt >= '2024-03-29' AND curated_weather.dt <= '2024-04-02'
GROUP BY curated_city.name;




-- Rank the most common weather conditions in a certain period of time per city?
SELECT
    city_name AS city,
    weather_condition,
    frequency,
    RANK() OVER (PARTITION BY city_name ORDER BY frequency DESC) AS weather_rank
FROM (
    SELECT
        curated_city.name AS city_name,
        curated_weather.main AS weather_condition,
        COUNT(*) AS frequency
    FROM
        curated_weather 
    JOIN
        curated_city ON curated_weather.city_id = curated_city.city_id
    WHERE curated_weather.dt >= '2024-03-29' AND curated_weather.dt <= '2024-04-02'
    GROUP BY
        curated_city.name,
        curated_weather.main
) ranked_weather;




-- What are the temperature averages observed in a certain period per city?
SELECT
    curated_city.name AS city,
    AVG(curated_weather.temperature) AS avg_temperature
FROM
    curated_weather
JOIN
    curated_city ON  curated_city.city_id = curated_weather.city_id
WHERE curated_weather.dt >= '2024-03-29' AND curated_weather.dt <= '2024-04-02'
GROUP BY curated_city.name;





-- What city had the highest absolute temperature in a certain period of time?
SELECT
    curated_city.name AS city,
    MAX(curated_weather.temperature) AS max_abs_temperature
FROM
    curated_weather
JOIN
    curated_city ON  curated_city.city_id = curated_weather.city_id
WHERE curated_weather.dt >= '2024-03-29' AND curated_weather.dt <= '2024-04-02'
GROUP BY curated_city.name
ORDER BY max_abs_temperature DESC
LIMIT 1;




-- What is the highest daily temperature variation in a certain period of time per day?
SELECT
    "DATE",
    city,
    temperature_variation
FROM (
    SELECT
        curated_city.name AS city,
        TO_DATE(curated_weather.dt) AS "DATE",
        MAX(curated_weather.temperature) - MIN(curated_weather.temperature) AS temperature_variation,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(curated_weather.dt) ORDER BY MAX(curated_weather.temperature) - MIN(curated_weather.temperature) DESC) AS row_num_var
    FROM
        curated_weather
    JOIN
        curated_city ON  curated_city.city_id = curated_weather.city_id
    WHERE curated_weather.dt >= '2024-03-29' AND curated_weather.dt <= '2024-04-02'
    GROUP BY 1, 2
)max_var
WHERE row_num_var = 1;




-- Which city had the highest daily temperature variation in a certain period of time?
SELECT
    curated_city.name AS city,
    TO_DATE(curated_weather.dt) AS "DATE",
    MAX(curated_weather.temperature) - MIN(curated_weather.temperature) AS temperature_variation
FROM
    curated_weather
JOIN
    curated_city ON  curated_city.city_id = curated_weather.city_id
WHERE curated_weather.dt >= '2024-03-29' AND curated_weather.dt <= '2024-04-02'
GROUP BY 1, 2
ORDER BY temperature_variation DESC
LIMIT 1;



-- What city had the strongest wind in a certain period of time?
SELECT
    curated_city.name AS city,
    MAX(curated_weather.wind_speed) AS max_wind_speed
FROM
    curated_weather
INNER JOIN
    curated_city ON  curated_city.city_id = curated_weather.city_id
WHERE curated_weather.dt >= '2024-03-29' AND curated_weather.dt <= '2024-04-02'
GROUP BY 1
ORDER BY max_wind_speed DESC
LIMIT 1;













