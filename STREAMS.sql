USE DATABASE WEATHER;

-------------------------------------------------- LANDING_ZONE STREAMS --------------------------------------------------
USE SCHEMA landing_zone;

-- Create an append_only stream object for the table landing_json
CREATE OR REPLACE STREAM landing_stm_city    
ON TABLE landing_json
append_only = true;

-- Clone the first stream since the same data will be consumed by two tasks
CREATE OR REPLACE STREAM landing_stm_weather CLONE landing_stm_city;


SHOW STREAMS;

SELECT * FROM landing_stm_city;
SELECT * FROM landing_stm_weather;

-------------------------------------------------- CURATED_ZONE STREAMS --------------------------------------------------
USE SCHEMA curated_zone;


-- Create streams on curated_zone for curated_city table to cupture INSERT and UPDATE operations
CREATE OR REPLACE STREAM curated_city_stm 
ON TABLE curated_city;

-- Create an append_only stream object for the table curated_weather
CREATE OR REPLACE STREAM curated_weather_stm1 
ON TABLE curated_weather
append_only = true;

-- Clone stream since the same data will be consumed by multiple tasks
CREATE OR REPLACE STREAM curated_weather_stm2 CLONE curated_weather_stm1;
CREATE OR REPLACE STREAM curated_weather_stm3 CLONE curated_weather_stm1;
CREATE OR REPLACE STREAM curated_weather_stm4 CLONE curated_weather_stm1;
CREATE OR REPLACE STREAM curated_weather_stm5 CLONE curated_weather_stm1;
CREATE OR REPLACE STREAM curated_weather_stm6 CLONE curated_weather_stm1;

SHOW STREAMS;

SELECT * FROM curated_city_stm;
SELECT * FROM curated_weather_stm1;
SELECT * FROM curated_weather_stm2;
SELECT * FROM curated_weather_stm3;
SELECT * FROM curated_weather_stm4;
SELECT * FROM curated_weather_stm5;
SELECT * FROM curated_weather_stm6;
