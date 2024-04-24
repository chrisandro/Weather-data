USE DATABASE weather;
USE SCHEMA landing_zone;

CREATE OR REPLACE PIPE weather_pipe
AUTO_INGEST = TRUE
AS
COPY INTO landing_json
FROM @weather_int_stg
FILE_FORMAT = (TYPE = 'JSON');