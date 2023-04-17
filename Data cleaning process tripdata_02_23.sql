####################################################################### DATA SETUP #######################################################################

/* Create a new schema to put our data into */
CREATE SCHEMA bikeshare;

/* Set bikeshare as the active schema */
USE bikeshare;

/* Create a table */
CREATE TABLE tripdata_02_22 (
    ride_id TEXT,
    rideable_type TEXT,
    started_at DATETIME,
    ended_at DATETIME,
    start_station_name TEXT,
    start_station_id TEXT,
    end_station_name TEXT,
    end_station_id TEXT,
    start_lat TEXT,
    start_lng TEXT,
    end_lat TEXT,
    end_lng TEXT,
    member_casual TEXT
);

/* Adding data to the table from the CSV file */
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/2022-01-divvy-tripdata-raw.csv'
INTO TABLE tripdata_02_22
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

/* Check if all the data got uploaded to the table */
SELECT COUNT(*)
FROM tripdata_02_22;-- the result needs to be the same as the number of rows in the CSV file

####################################################################### DATA CLEANING #######################################################################
/* Check a sample from the table */
SELECT *
FROM tripdata_23_02
LIMIT 100;

/* Check the columns for NULL values */
-- ride_id: result -> no nulls
SELECT *
FROM tripdata_23_02
WHERE ride_id = '' OR ride_id IS NULL;-- Sometimes empty cells don't register as null in MySQL, so IS NULL clause doesn't always work
-- rideable_type: result -> no nulls
SELECT *
FROM tripdata_23_02
WHERE rideable_type = '' OR ride_id IS NULL;
-- started_at: result -> no nulls
SELECT *
FROM tripdata_23_02
WHERE started_at IS NULL;
-- ended_at: result -> no nulls
SELECT *
FROM tripdata_23_02
WHERE ended_at IS NULL;
-- start_station_name: result -> there are nulls
SELECT *
FROM tripdata_23_02
WHERE start_station_name = '' OR start_station_name IS NULL;
-- start_station_id: result -> there are nulls
SELECT *
FROM tripdata_23_02
WHERE start_station_id = '' OR start_station_id IS NULL;
-- end_station_name: result -> there are nulls
SELECT *
FROM tripdata_23_02
WHERE end_station_name = '' OR end_station_name IS NULL;
-- end_station_id: result -> there are nulls
SELECT *
FROM tripdata_23_02
WHERE end_station_id = '' OR end_station_id IS NULL;
-- start_lat: result -> no nulls
SELECT *
FROM tripdata_23_02
WHERE start_lat = '' OR start_lat IS NULL;
-- start_lng: result -> no nulls
SELECT *
FROM tripdata_23_02
WHERE start_lng = '' OR start_lng IS NULL;
-- end_lat: result -> there are nulls
SELECT *
FROM tripdata_23_02
WHERE end_lat = '' OR start_lat IS NULL;
-- end_lng: result -> there are nulls
SELECT *
FROM tripdata_23_02
WHERE end_lng = '' OR end_lng IS NULL;
-- member_casual: result -> no nulls
SELECT *
FROM tripdata_23_02
WHERE member_casual = '' OR member_casual IS NULL;

/* Check for duplicates in ride_id */
SELECT COUNT(DISTINCT ride_id), COUNT(*)
FROM tripdata_23_02; -- result -> no duplicate IDs

/* Check for typos in rideable_type */
SELECT DISTINCT rideable_type
FROM tripdata_23_02;-- result -> no typos

/*check for wrong time values */
SELECT ride_id, started_at, ended_at
FROM tripdata_23_02
WHERE started_at > ended_at;-- result -> not empty, one row where the end time comes before the start time.
-- Fix the wrong times by switching them around
WITH timeCTE as(
SELECT ride_id, started_at AS end, ended_at AS start FROM tripdata_22_11 WHERE started_at > ended_at
)
UPDATE tripdata_22_11 AS a JOIN timeCTE AS b ON a.ride_id = b.ride_id
SET started_at = start, ended_at = end
where a.ride_id = b.ride_id;

/* Check for station IDs with multiple names */
SELECT COUNT(DISTINCT start_station_name), COUNT(DISTINCT start_station_id)
FROM tripdata_23_02; -- result -> there are different values with same station id
SELECT COUNT(DISTINCT end_station_name), COUNT(DISTINCT end_station_id)
FROM tripdata_23_02; -- result -> there are different values with same station id
-- Step 2: Find which ids have different names
WITH temp_station AS(
SELECT DISTINCT start_station_name, start_station_id
FROM tripdata_23_02
)
SELECT DISTINCT a.start_station_name, a.start_station_id
FROM temp_station AS a
        JOIN
    temp_station AS b ON a.start_station_id = b.start_station_id
WHERE
    a.start_station_id != b.start_station_id
        AND a.start_station_name = b.start_station_name
        OR a.start_station_id = b.start_station_id
        AND a.start_station_name != b.start_station_name
ORDER BY start_station_id;

WITH temp_station AS(
SELECT DISTINCT end_station_name, end_station_id
FROM tripdata_23_02
)
SELECT DISTINCT a.end_station_name, a.end_station_id
FROM temp_station AS a
        JOIN
    temp_station AS b ON a.end_station_id = b.end_station_id
WHERE a.end_station_id != b.end_station_id
        AND a.end_station_name = b.end_station_name
        OR a.end_station_id = b.end_station_id
        AND a.end_station_name != b.end_station_name
ORDER BY end_station_id;

/* Check for typos in member_casual */
SELECT DISTINCT member_casual
FROM tripdata_23_02; -- result -> only 2 values returned, no typos

/* Check for duplicate rows */
WITH RowNumCTE AS (
SELECT *, row_number() OVER (PARTITION BY rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual  ORDER BY ride_id) AS row_num
FROM tripdata_23_02) -- All rows with the same rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual are considered duplicates
SELECT *
FROM RowNumCTE
WHERE row_num >1; -- result -> we found 1 duplicate row
-- Deleting duplicate rows (Note that removing data directly from the source table isn't standard practice. It's done here for demonstration purposes only)
WITH RowNumCTE AS (
SELECT *, row_number() OVER (PARTITION BY rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual  ORDER BY ride_id) AS row_num
FROM tripdata_23_02)
DELETE FROM tripdata_23_02 USING tripdata_23_02 JOIN RowNumCTE ON tripdata_23_02.ride_id = RowNumCTE.ride_id
WHERE RowNumCTE.row_num >1;

/* Dropping the location columns (the data is to dirty to be cleaned effectively or used as is) */
ALTER TABLE tripdata_23_02
DROP COLUMN start_station_name, 
DROP COLUMN start_station_id, 
DROP COLUMN end_station_name, 
DROP COLUMN end_station_id,
DROP COLUMN start_lat, 
DROP COLUMN start_lng, 
DROP COLUMN end_lat, 
DROP COLUMN end_lng;

/* Export the data into an CSV file */
SELECT 'ride_id', 'rideable_type', 'started_at', 'ended_at', 'member_casual'
UNION ALL
SELECT * FROM tripdata_23_02
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/tripdata_23_02_clean.csv'
FIELDS ENCLOSED BY '' 
TERMINATED BY ',' 
ESCAPED BY '' 
LINES TERMINATED BY '\n';
