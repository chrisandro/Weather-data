USE DATABASE weather;

-------------------------------------------------- CURATED_ZONE TASKS --------------------------------------------------
USE SCHEMA curated_zone;


-- Task that updates/inserts data to the curated_city table
CREATE OR REPLACE TASK curated_city_task
    warehouse = COMPUTE_WH
    schedule = '1 minute'
WHEN
    system$stream_has_data('weather.landing_zone.landing_stm_city')
AS
    MERGE INTO weather.curated_zone.curated_city curated_city
    USING weather.landing_zone.landing_stm_city landing_stm_city ON
    curated_city.name = landing_stm_city.weather_data:name::string
    
-- If the flat IDs match then UPDATE the listing
WHEN MATCHED THEN
    UPDATE SET
        curated_city.city_id = landing_stm_city.weather_data:id::integer
        
-- If the flat IDs DO NOT match then INSERT the new listing
WHEN NOT MATCHED THEN        
    INSERT (
        city_id, name, timezone, country, latitude, lonigtude
    )
    VALUES (
        landing_stm_city.weather_data:id::integer,
        landing_stm_city.weather_data:name::string,
        landing_stm_city.weather_data:timezone::integer,
        CASE
        WHEN landing_stm_city.weather_data:timezone::integer <> 3600 THEN 3600
        ELSE landing_stm_city.weather_data:timezone::integer
        END,
        landing_stm_city.weather_data:country::string,
        landing_stm_city.weather_data:lat::float,
        landing_stm_city.weather_data:lon::float
    );


    
-- Task that updates/inserts data to the curated_city table
CREATE OR REPLACE TASK curated_weather_task
    warehouse = COMPUTE_WH
    schedule = '2 minute'
WHEN
    system$stream_has_data('weather.landing_zone.landing_stm_weather')
AS
    INSERT INTO weather.curated_zone.curated_weather (
        city_id, main, description, temperature, feels_like, temp_min, temp_max, pressure, humidity,
        visibility, wind_speed, wind_direction, cloudiness, rain_1h, rain_3h, snow_1h, snow_3h, dt
    )
    VALUES (
        landing_stm_weather.weather_data:id::integer,
        landing_stm_weather.weather_data:weather[0].main::string,
        landing_stm_weather.weather_data:weather[0].description::string,
        landing_stm_weather.weather_data:main.temp::float,
        landing_stm_weather.weather_data:main.feels_like::float,
        landing_stm_weather.weather_data:main.temp_min::float,
        landing_stm_weather.weather_data:main.temp_max::float,
        landing_stm_weather.weather_data:main.pressure::float,
        landing_stm_weather.weather_data:main.humidity::float,
        landing_stm_weather.weather_data:visibility::integer,
        landing_stm_weather.weather_data:wind.speed::float,
        CASE
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 337.5 OR landing_stm_weather.weather_data:wind.deg::float < 22.5 THEN 'North'
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 22.5 AND landing_stm_weather.weather_data:wind.deg::float < 67.5 THEN 'North-East'
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 67.5 AND landing_stm_weather.weather_data:wind.deg::float < 112.5 THEN 'East'
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 112.5 AND landing_stm_weather.weather_data:wind.deg::float < 157.5 THEN 'South-East'
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 157.5 AND landing_stm_weather.weather_data:wind.deg::float < 202.5 THEN 'South'
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 202.5 AND landing_stm_weather.weather_data:wind.deg::float < 247.5 THEN 'South-West'
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 247.5 AND landing_stm_weather.weather_data:wind.deg::float < 292.5 THEN 'West'
        WHEN landing_stm_weather.weather_data:wind.deg::float >= 292.5 AND landing_stm_weather.weather_data:wind.deg::float < 337.5 THEN 'North-West'
        END,
        landing_stm_weather.weather_data:clouds.all::float,
        landing_stm_weather.weather_data:rain."1h"::float,
        landing_stm_weather.weather_data:rain."3h"::float,
        landing_stm_weather.weather_data:snow."1h"::float,
        landing_stm_weather.weather_data:snow."3h"::float,
        TO_TIMESTAMP(weather_data:dt::integer)
    );

-----------------------------------------------------------------------------------------------------------------------------
   
SHOW TASKS;

-- Resume the tasks so it starts running
ALTER TASK curated_city_task RESUME;
ALTER TASK curated_weather_task RESUME;

-- See the status via tasks history
SELECT * FROM TABLE (information_schema.task_history())
WHERE name IN ('CURATED_CITY_TASK', 'CURATED_WEATHER_TASK')
ORDER BY scheduled_time;

-------------------------------------------------- CONSUMPTION_ZONE TASKS --------------------------------------------------
USE SCHEMA consumption_zone;

-- Task that updates/inserts data to the consumption_city table
CREATE OR REPLACE TASK consumption_city_task
    warehouse = COMPUTE_WH
    schedule = '3 minute'
WHEN
    system$stream_has_data('weather.curated_zone.curated_city_stm')
AS
    MERGE INTO weather.consumption_zone.consumption_city consumption_city
    USING weather.curated_zone.curated_city_stm curated_city_stm ON
    consumption_city.name = curated_city_stm.name::string

-- If the flat IDs match then UPDATE the listing
WHEN MATCHED THEN
    UPDATE SET
        consumption_city.city_id = curated_city_stm.id::integer,
        consumption_city.timezone = CASE
                                    WHEN curated_city_stm.timezone::integer = 0 THEN 'UTC+00:00'
                                    WHEN curated_city_stm.timezone::integer = 3600 THEN 'UTC+01:00'
                                    WHEN curated_city_stm.timezone::integer = 7200 THEN 'UTC+02:00'
                                    END
                                    
-- If the flat IDs DO NOT match then INSERT the new listing
WHEN NOT MATCHED THEN        
    INSERT (
        city_id, name, timezone, country, latitude, lonigtude
    )
    VALUES (
        curated_city_stm.id::integer,
        curated_city_stm.name::string,
        curated_city_stm.timezone::integer,
        CASE
        WHEN curated_city_stm.timezone::integer = 0 THEN 'UTC+00:00'
        WHEN curated_city_stm.timezone::integer = 3600 THEN 'UTC+01:00'
        WHEN curated_city_stm.timezone::integer = 7200 THEN 'UTC+02:00'
        END,
        curated_city_stm.country::string,
        curated_city_stm.lat::float,
        curated_city_stm.lon::float
    );

    
-- Task that updates data to the distinct_weather_conditions table
CREATE OR REPLACE TASK distinct_weather_task
    warehouse = COMPUTE_WH
    schedule = '4 minute'
WHEN
    system$stream_has_data('weather.curated_zone.curated_weather_stm1')
AS
    MERGE INTO weather.consumption_zone.distinct_weather_conditions distinct_weather_conditions
    USING weather.curated_zone.curated_weather_stm1 curated_weather_stm1 ON
    array_contains(distinct_weather_conditions.distinct_conditions, curated_weather_stm1.main::string)

-- If there is a new weather condition that hasn't been recorded yet, update the result of the table
WHEN NOT MATCHED THEN
    UPDATE SET
        distinct_weather_conditions.distinct_conditions_count = distinct_weather_conditions.distinct_conditions_count + 1,
        distinct_weather_conditions.distinct_conditions = array_append(distinct_weather_conditions.distinct_conditions, curated_weather_stm1.main::string);


-- Task that updates/inserts data to the weather_conditions_rank_per_city table
CREATE OR REPLACE TASK weather_rank_task
    warehouse = COMPUTE_WH
    schedule = '5 minute'
WHEN
    system$stream_has_data('weather.curated_zone.curated_weather_stm2')
AS
    MERGE INTO weather.consumption_zone.weather_conditions_rank_per_city weather_conditions_rank_per_city
    USING weather.curated_zone.curated_weather_stm2 curated_weather_stm2 ON
    weather_conditions_rank_per_city.city_id = curated_weather_stm2.city_id AND
    weather_conditions_rank_per_city.weather_condition = curated_weather_stm2.main

WHEN MATCHED THEN
    UPDATE SET
        weather_conditions_rank_per_city.frequency = weather_conditions_rank_per_city.frequency + 1
        --weather_conditions_rank_per_city.weather_rank =  RANK() OVER (PARTITION BY weather_conditions_rank_per_city.city_id ORDER BY weather_conditions_rank_per_city.frequency DESC)

WHEN NOT MATCHED THEN
    INSERT (
        city_id, city, weather_condition, frequency
    )
    VALUES (
        curated_weather_stm2.city_id::integer,
        (SELECT name FROM consumption_zone.consumption_city WHERE consumption_zone.consumption_city.city_id = curated_weather_stm2.city_id),
        curated_weather_stm2.main::string,
        1       
    );


-- Task that updates/inserts data to the avg_temp_per_city table
CREATE OR REPLACE TASK avg_temp_task
    warehouse = COMPUTE_WH
    schedule = '6 minute'
WHEN
    system$stream_has_data('weather.curated_zone.curated_weather_stm3')
AS
    MERGE INTO weather.consumption_zone.avg_temp_per_city avg_temp_per_city
    USING weather.curated_zone.curated_weather_stm3 curated_weather_stm3 ON
    avg_temp_per_city.city_id = curated_weather_stm3.city_id

WHEN MATCHED THEN
    UPDATE SET
        avg_temp_per_city.avg_temperature = (avg_temp_per_city.avg_temperature + curated_weather_stm3.temperature) / 2

WHEN NOT MATCHED THEN
    INSERT (
        city_id, city, avg_temperature
    )
    VALUES (
        curated_weather_stm3.city_id::integer,
        (SELECT name FROM consumption_zone.consumption_city WHERE consumption_zone.consumption_city.city_id = curated_weather_stm3.city_id),
        curated_weather_stm3.temperature
    );
    

-- Task that updates data to the max_abs_temp table
CREATE OR REPLACE TASK max_abs_temp_task
    warehouse = COMPUTE_WH
    schedule = '7 minute'
WHEN
    system$stream_has_data('weather.curated_zone.curated_weather_stm4')
AS
    MERGE INTO weather.consumption_zone.max_abs_temp max_abs_temp
    USING weather.curated_zone.curated_weather_stm4 curated_weather_stm4 ON
    max_abs_temp.max_abs_temperature < curated_weather_stm4.temperature

WHEN MATCHED THEN
    UPDATE SET
        max_abs_temp.max_abs_temperature = curated_weather_stm4.temperature;
        


-- Task that updates data to the city_with_strongest_wind table
CREATE OR REPLACE TASK strongest_wind_task
    warehouse = COMPUTE_WH
    schedule = '8 minute'
WHEN
    system$stream_has_data('weather.curated_zone.curated_weather_stm5')
AS
    MERGE INTO weather.consumption_zone.city_with_strongest_wind city_with_strongest_wind
    USING weather.curated_zone.curated_weather_stm5 curated_weather_stm5 ON
    city_with_strongest_wind.max_wind_speed < curated_weather_stm5.wind_speed

WHEN MATCHED THEN
    UPDATE SET
        city_with_strongest_wind.max_wind_speed = curated_weather_stm5.wind_speed;

-----------------------------------------------------------------------------------------------------------------------------

USE SCHEMA weather.consumption_zone;

SHOW TASKS;

-- Resume the tasks so it starts running
ALTER TASK consumption_city_task RESUME;
ALTER TASK distinct_weather_task RESUME;
ALTER TASK weather_rank_task RESUME;
ALTER TASK avg_temp_task RESUME;
ALTER TASK max_abs_temp_task RESUME;
ALTER TASK strongest_wind_task RESUME;


-- See the status via tasks history
SELECT * FROM TABLE (information_schema.task_history())
WHERE name IN ('CONSUMPTION_CITY_TASK', 'DISTINCT_WEATHER_TASK', 'WEATHER_RANK_TASK', 'AVG_TEMP_TASK', 'MAX_ABS_TEMP_TASK', 'STRONGEST_WIND_TASK')
ORDER BY scheduled_time;
