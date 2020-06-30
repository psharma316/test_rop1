CREATE EXTERNAL TABLE stagingdb1.chicago_taxi_trips_csv2(
unique_key   STRING,
taxi_id  STRING,
trip_start_timestamp  TIMESTAMP,
trip_end_timestamp  TIMESTAMP,
trip_seconds  INT,
trip_miles   FLOAT,
pickup_census_tract  INT,
dropoff_census_tract  INT,
pickup_community_area  INT,
dropoff_community_area  INT,
fare  FLOAT,
tips  FLOAT,
tolls  FLOAT,
extras  FLOAT,
trip_total  FLOAT,
payment_type  STRING,
company  STRING,
pickup_latitude  FLOAT,
pickup_longitude  FLOAT,
pickup_location  STRING,
dropoff_latitude  FLOAT,
dropoff_longitude  FLOAT,
dropoff_location  STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location 'gs://data-bucket-hive2/chicago_taxi_trips/csv/';
