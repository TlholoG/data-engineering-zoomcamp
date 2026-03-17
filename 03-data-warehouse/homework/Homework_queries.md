Question 1.
SELECT count(*) FROM `kestra-sandbox-487614.zoomcamphomework.yellow_tripdata` 

Question 2.
SELECT count(DISTINCT PULocationID) FROM `kestra-sandbox-487614.zoomcamphomework.yellow_tripdata` 

SELECT count(Distinct PULocationID) FROM `kestra-sandbox-487614.zoomcamphomework.yellow_tripdata_2024_01_ext` 

Question 3.
SELECT  PULocationID 
FROM `kestra-sandbox-487614.zoomcamphomework.yellow_tripdata` ;

Question 4.
SELECT  PULocationID, DOLocationID 
FROM `kestra-sandbox-487614.zoomcamphomework.yellow_tripdata` ;

Question 5.
SELECT  count(fare_amount) 
FROM `kestra-sandbox-487614.zoomcamphomework.yellow_tripdata` 
WHERE fare_amount = 0;

Question 6.
SELECT  DISTINCT(VendorID) 
FROM `kestra-sandbox-487614.zoomcamphomework.yellow_tripdata` 
WHERE tpep_dropoff_datetime > '2024-03-01'
AND  tpep_dropoff_datetime <= '2024-03-15';

Question 7.


