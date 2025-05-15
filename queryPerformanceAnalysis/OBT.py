from pyspark.sql import SparkSession
from pyspark import SparkConf
import time

conf = SparkConf().setAppName("OBT Query Performance Analysis").setMaster("local[8]").set("spark.driver.memory", "4g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

feb2022_trips = spark.read.csv("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/results/Feb2022_cleaned.csv", header=True)


feb2022_trips.createOrReplaceTempView("feb2022_trips")

start_time = time.perf_counter_ns()

# Query 1: Find the average trip distance and average trip duration of each vehicle_id in Feb
query = """
    SELECT VEHICLE_ID, EXTRACT(MONTH FROM DT_START) AS Month, AVG(distance_km) AS average_distance_per_month, AVG(duration_s) AS average_duration_per_month
    FROM feb2022_trips
    GROUP BY VEHICLE_ID, Month
    ORDER BY VEHICLE_ID, Month;
"""

# Query 2: Most common pickup locations
query = """
    SELECT latitude_pickup, longitude_pickup, COUNT(*) AS num_pickups
    FROM feb2022_trips
    GROUP BY latitude_pickup, longitude_pickup
    ORDER BY num_pickups DESC;
"""

# Query 3: Most common dropoff locations
query = """
    SELECT latitude_end, longitude_end, COUNT(*) AS num_dropoffs
    FROM feb2022_trips
    GROUP BY latitude_end, longitude_end
    ORDER BY num_dropoffs DESC;
"""

# Query 4: Most common pickup time
query = """
    SELECT EXTRACT(HOUR FROM DT_START) AS pickup_hour, COUNT(*) AS num_hour_pickups
    FROM feb2022_trips
    GROUP BY pickup_hour
    ORDER BY num_hour_pickups DESC;
"""

# Query 5: Most common dropoff time
query = """
    SELECT EXTRACT(HOUR FROM DT_END) AS dropoff_hour, COUNT(*) AS num_hour_dropoffs
    FROM feb2022_trips
    GROUP BY dropoff_hour
    ORDER BY num_hour_dropoffs DESC;
"""

# Ad hoc Queries 
# Query 1: Get all trip records of 50% of the vehicles
query = """
    WITH sampled_vehicle AS (
        SELECT VEHICLE_ID
        FROM (SELECT VEHICLE_ID, ROW_NUMBER() OVER (ORDER BY VEHICLE_ID) AS rn FROM (SELECT DISTINCT VEHICLE_ID FROM feb2022_trips))
        WHERE MOD(rn, 2) = 0
    )
    SELECT *
    FROM feb2022_trips f
    JOIN sampled_vehicle v 
    ON f.VEHICLE_ID = v.VEHICLE_ID;
"""

# Query 2: All trips made on a specific day (e.g. 12)
query = """
    SELECT *
    FROM feb2022_trips f
    WHERE EXTRACT(DAY FROM DT_START) = 12;
"""

# # Query 3: All trips made by a specific vehicle_id 
query = """
    SELECT *
    FROM feb2022_trips f
    WHERE VEHICLE_ID = "cb9b028e8d56db24693add4f08da44070e66b7b8fca97ca66be96bf52926c925";
"""

end_time = time.perf_counter_ns()
result_df = spark.sql(query)

# result_df.explain(True)
result_df.show()
execution_time_ns = end_time - start_time
execution_time_us = execution_time_ns / 1_000  
execution_time_ms = execution_time_ns / 1_000_000 
print(f"Execution Time: {execution_time_ns} ns ({execution_time_us:.3f} µs / {execution_time_ms:.3f} ms)")


spark.stop()
