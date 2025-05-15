from pyspark.sql import SparkSession
from pyspark import SparkConf
import time

conf = SparkConf().setAppName("Fact Dimension Query Performance Analysis").setMaster("local[8]").set("spark.driver.memory", "4g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

fact_trips = spark.read.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/fact_trips.parquet")
dim_vehicle = spark.read.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_vehicle.parquet")
dim_driver = spark.read.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_driver.parquet")
dim_dropoff_location = spark.read.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_dropoff_location.parquet")
dim_dropoff_time = spark.read.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_dropoff_time.parquet")
dim_pickup_location = spark.read.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_pickup_location.parquet")
dim_pickup_time = spark.read.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_pickup_time.parquet")

fact_trips.createOrReplaceTempView("fact_trips")
dim_vehicle.createOrReplaceTempView("dim_vehicle")
dim_driver.createOrReplaceTempView("dim_driver")
dim_dropoff_location.createOrReplaceTempView("dim_dropoff_location")
dim_dropoff_time.createOrReplaceTempView("dim_dropoff_time")
dim_pickup_location.createOrReplaceTempView("dim_pickup_location")
dim_pickup_time.createOrReplaceTempView("dim_pickup_time")

start_time = time.perf_counter_ns()

# Analytical Queries
# # Query 1: Find the average trip distance and average trip duration of each vehicle_id in Feb
# query = """
#     SELECT f.VEHICLE_ID, t.month AS Month, AVG(f.distance_km) AS average_distance_per_month, AVG(f.duration_s) AS average_trip_duration
#     FROM fact_trips f
#     JOIN dim_pickup_time t ON f.PICKUP_TIME_ID = t.PICKUP_TIME_ID
#     GROUP BY f.VEHICLE_ID, Month
#     ORDER BY f.VEHICLE_ID, Month;
# """

# # Query 2: Most common pickup locations
# query = """
#     SELECT pu.latitude_pickup, pu.longitude_pickup, COUNT(*) AS num_pickups
#     FROM fact_trips f
#     JOIN dim_pickup_location pu
#     ON f.PICKUP_LOCATION_ID = pu.PICKUP_LOCATION_ID
#     GROUP BY pu.latitude_pickup, pu.longitude_pickup
#     ORDER BY num_pickups DESC;
# """

# # Query 3: Most common dropoff locations
# query = """
#     SELECT do.latitude_dropoff, do.longitude_dropoff, COUNT(*) AS num_dropoffs
#     FROM fact_trips f
#     JOIN dim_dropoff_location do
#     ON f.DROPOFF_LOCATION_ID = do.DROPOFF_LOCATION_ID
#     GROUP BY do.latitude_dropoff, do.longitude_dropoff
#     ORDER BY num_dropoffs DESC;
# """

# # Query 4: Most common pickup time
# query = """
#     SELECT EXTRACT(HOUR FROM p.pickup_dt) AS pickup_hour, COUNT(*) AS num_hour_pickups
#     FROM fact_trips f
#     JOIN dim_pickup_time p
#     ON f.PICKUP_TIME_ID = p.pickup_time_id
#     GROUP BY EXTRACT(HOUR FROM p.pickup_dt)
#     ORDER BY num_hour_pickups DESC;
# """

# # Query 5: Most common dropoff time
# query = """
#     SELECT EXTRACT(HOUR FROM d.dropoff_dt) AS dropoff_hour, COUNT(*) AS num_hour_dropoffs
#     FROM fact_trips f
#     JOIN dim_dropoff_time d
#     ON f.DROPOFF_TIME_ID = d.dropoff_time_id
#     GROUP BY EXTRACT(HOUR FROM d.dropoff_dt)
#     ORDER BY num_hour_dropoffs DESC;
# """

# # Ad Hoc Queries
# # Query 1: Get all trip records of 50% of the vehicles
# query = """
#     WITH sampled_vehicle AS (
#     SELECT VEHICLE_ID
#     FROM (SELECT VEHICLE_ID, ROW_NUMBER() OVER (ORDER BY VEHICLE_ID) AS rn FROM dim_vehicle)
#     WHERE MOD(rn, 2) = 0
#     )

#     SELECT *
#     FROM fact_trips f
#     JOIN sampled_vehicle v ON f.VEHICLE_ID = v.VEHICLE_ID
#     JOIN dim_pickup_time pt ON f.PICKUP_TIME_ID = pt.PICKUP_TIME_ID
#     JOIN dim_dropoff_time do ON f.DROPOFF_TIME_ID = do.DROPOFF_TIME_ID
#     JOIN dim_pickup_location pu ON f.PICKUP_LOCATION_ID = pu.PICKUP_LOCATION_ID
#     JOIN dim_dropoff_location dl ON f.DROPOFF_LOCATION_ID = dl.DROPOFF_LOCATION_ID;
# """

# # Query 2: All trips made on a specific day (e.g. 12)
# query = """
#     SELECT *
#     FROM fact_trips f
#     JOIN dim_pickup_time pt ON f.PICKUP_TIME_ID = pt.PICKUP_TIME_ID
#     JOIN dim_dropoff_time do ON f.DROPOFF_TIME_ID = do.DROPOFF_TIME_ID
#     JOIN dim_pickup_location pu ON f.PICKUP_LOCATION_ID = pu.PICKUP_LOCATION_ID
#     JOIN dim_dropoff_location dl ON f.DROPOFF_LOCATION_ID = dl.DROPOFF_LOCATION_ID
#     WHERE pt.day = 12;
# """

# # Query 3: All trips made by a specific vehicle_id 
query = """
    SELECT *
    FROM fact_trips f
    JOIN dim_pickup_time pt ON f.PICKUP_TIME_ID = pt.PICKUP_TIME_ID
    JOIN dim_dropoff_time do ON f.DROPOFF_TIME_ID = do.DROPOFF_TIME_ID
    JOIN dim_pickup_location pu ON f.PICKUP_LOCATION_ID = pu.PICKUP_LOCATION_ID
    JOIN dim_dropoff_location dl ON f.DROPOFF_LOCATION_ID = dl.DROPOFF_LOCATION_ID
    WHERE f.vehicle_id = "cb9b028e8d56db24693add4f08da44070e66b7b8fca97ca66be96bf52926c925";
"""

result_df = spark.sql(query)

# result_df.explain(True)
result_df.show()
end_time = time.perf_counter_ns()

execution_time_ns = end_time - start_time
execution_time_us = execution_time_ns / 1_000  
execution_time_ms = execution_time_ns / 1_000_000 
print(f"Execution Time: {execution_time_ns} ns ({execution_time_us:.3f} µs / {execution_time_ms:.3f} ms)")

spark.stop()