INSERT OVERWRITE TABLE stagingdb1.chicago_taxi_trips_parquet \
SELECT * FROM stagingdb1.chicago_taxi_trips_csv;