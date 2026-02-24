-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-487614.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-tlholo-demo/yellow_tripdata_2019-*.csv', 'gs://kestra-zoomcamp-tlholo-demo/yellow_tripdata_2020-*.csv']
);

SELECT *  FROM `kestra-sandbox-487614.zoomcamp.external_yellow_tripdata` LIMIT 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE kestra-sandbox-487614.zoomcamp.yellow_tripdata_non_partitioned AS
SELECT * FROM kestra-sandbox-487614.zoomcamp.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE kestra-sandbox-487614.zoomcamp.yellow_tripdata_partitioned 
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM kestra-sandbox-487614.zoomcamp.external_yellow_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM kestra-sandbox-487614.zoomcamp.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;