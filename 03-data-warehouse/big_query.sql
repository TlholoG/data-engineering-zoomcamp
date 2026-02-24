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

SELECT *  FROM `kestra-sandbox-487614.zoomcamp.external_yellow_tripdata` LIMIT 10