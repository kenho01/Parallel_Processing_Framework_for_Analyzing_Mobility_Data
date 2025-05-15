# This is the main DEBUGGING python file to perform data processing with PySpark
# There are alot of overhead induced from .count()

# Import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from shapely.geometry import shape, Point
from pyspark.sql.types import IntegerType
import geopandas as gpd
import json
import os
import sys
import time
import math

def main():
    # Set paths
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Initialize & Configure Spark session
    conf = SparkConf().setAppName("Mobility Data Process Optimisation").setMaster("local[8]")
                        # .setMaster("spark://192.168.1.6:7077") -> Spark Standalone Cluster 
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    conf = spark.sparkContext.getConf()

    # Read input csv file
    df_raw = spark.read.option("header", True)\
            .csv("resources/Sep2022_sample.csv")
    # Convert df to a temporary view
    df_raw.createOrReplaceTempView("mobility_table")

    # START OF DATA CLEANING

    # Step 1: Cast datetime from string to timestamp, latitude & longitude to float
    query = """
    SELECT 
        VEHICLE_ID, REQUEST_ID, DRIVER_ID, CAST(LATITUDE_PICKUP AS NUMERIC(11,8)) AS LATITUDE_PICKUP, CAST(LONGITUDE_PICKUP AS NUMERIC(11,8)) AS LONGITUDE_PICKUP
        ,CAST(LATITUDE_END AS NUMERIC(11,8)) AS LATITUDE_END, CAST(LONGITUDE_END AS NUMERIC(11,8)) AS LONGITUDE_END, 
        to_timestamp(DT_START,'yyyy-MM-dd HH:mm:ss') AS START_TIME, to_timestamp(DT_END,'yyyy-dd-MM HH:mm:ss') AS END_TIME
    FROM mobility_table
    """
    # Execute query
    df_tempview = spark.sql(query)

    # Step 2: Remove rows with NULL values for latitude & longitude 
    prev = df_tempview.count()
    print("No. of trips before removing NULL latitude & longitude: ", df_tempview.count())
    df_tempview = df_tempview.dropna()
    # Replace view
    df_tempview.createOrReplaceTempView("mobility_table")
    print("No. of trips after removing NULL latitude & longitude: ", df_tempview.count())
    next = df_tempview.count()
    print("No. of trips removed: ", prev - next)

    # Step 3: Check for off-boundary pick ups & dropoffs  
    # Read geojson file
    gdf = gpd.read_file("resources/MasterPlan2019PlanningAreaBoundaryNoSea.geojson")
    polygons = [shape(feature["geometry"]) for feature in json.loads(gdf.to_json())["features"]]  

    # User defined function: returns 1 if given latitude & longitude is within SG's boundary, else return 0
    @udf(returnType=IntegerType())
    def within_boundary(latitude, longitude):
        point = Point(longitude, latitude)
        return 1 if any(polygon.contains(point) for polygon in polygons) else 0
    spark.udf.register("within_boundary", within_boundary)

    query = """
    SELECT *, within_boundary(LATITUDE_PICKUP, LONGITUDE_PICKUP) AS WITHIN_BOUNDARY_PICKUP, 
            within_boundary(LATITUDE_END, LONGITUDE_END) AS WITHIN_BOUNDARY_END
    FROM mobility_table
    """
    # Execute query
    df_tempview = spark.sql(query)
    # Replace view
    df_tempview.createOrReplaceTempView("mobility_table")

    prev = df_tempview.count()
    print("No. of rows before removing off boundary pickup & dropoff: ", df_tempview.count())
    query = """
    SELECT 
        VEHICLE_ID,
        REQUEST_ID,
        DRIVER_ID, 
        START_TIME, 
        END_TIME, 
        LATITUDE_PICKUP, 
        LONGITUDE_PICKUP, 
        LATITUDE_END, 
        LONGITUDE_END 
    FROM 
        mobility_table
    WHERE
        WITHIN_BOUNDARY_PICKUP = 1
    AND
        WITHIN_BOUNDARY_END = 1
    """
    # Execute query
    df_tempview = spark.sql(query)
    # Replace view
    df_tempview.createOrReplaceTempView("mobility_table")
    next = df_tempview.count()
    print("No. of rows after removing off boundary pickup & dropoff: ", df_tempview.count())
    print("No. of trips removed: ", prev - next)

    # Step 4: Remove duplicates
        # 1. VEHICLE_ID is with 1 DRIVER_ID at a time
        # 2. Any trips that start before the previous trip end using the same VEHICLE_ID will be dropped
    # Repartition data by VEHICLE_ID to ensure all trips for a vehicle are in the same partition
    df_tempview = df_tempview.repartition("VEHICLE_ID")
    prev = df_tempview.count()
    print("No. of rows after removing duplicates: ", df_tempview.count())
    df_tempview = df_tempview.orderBy(["VEHICLE_ID", "START_TIME"])
    data_rdd = df_tempview.rdd
    def remove_duplicate_trips(partition):
        prev_vehicle = None
        prev_end_time = None
        data_to_keep = []

        for row in partition:
            if prev_vehicle == row['VEHICLE_ID']:
                if prev_end_time and prev_end_time > row['START_TIME']:
                    continue  
                else:
                    prev_end_time = row['END_TIME']
            else:
                prev_vehicle = row['VEHICLE_ID']
                prev_end_time = row['END_TIME']
            
            data_to_keep.append(row)
        
        return iter(data_to_keep)

    filtered_rdd = data_rdd.mapPartitions(remove_duplicate_trips)
    filtered_df = filtered_rdd.toDF(df_tempview.schema)
    filtered_df.createOrReplaceTempView("mobility_table")
    next = filtered_df.count()
    print("No. of rows after removing duplicates: ", filtered_df.count())
    print("No. of trips removed: ", prev - next)

    # START OF DATA PROCESSING

    # Step 5: Calculate distance travelled and time taken for each trip 
    # User defined function: calculate distance between 2 points
    @udf(returnType=DoubleType())
    def haversine_distance(lat1, lon1, lat2, lon2):
        R = 6371.0  

        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance
    spark.udf.register("haversine_distance", haversine_distance)

    # Calculate distance, duration of trip
    query = """
    SELECT *, haversine_distance(LATITUDE_PICKUP, LONGITUDE_PICKUP, LATITUDE_END, LONGITUDE_END) AS distance, 
    (unix_timestamp(END_TIME) - unix_timestamp(START_TIME)) AS duration_s
    FROM mobility_table
    """
    # Execute query
    df_tempview = spark.sql(query)
    # Replace view
    df_tempview.createOrReplaceTempView("mobility_table")

    # DEBUGGING QUERY
    # print("Debugging table:")
    # query = """
    # SELECT *
    # FROM mobility_table
    # WHERE VEHICLE_ID LIKE "9cbba%"
    # """
    # # Execute query
    # df_tempview = spark.sql(query)
    # df_tempview.show(df_tempview.count())
    
    # Step 6: Connect Trips -> USING SQL
    # Trips are considered to be connected if
        # 1. End of first trip is less than 1 minute from the start of the second trip
        # 2. Threshold of 6km
        # Assumption: A trip can have only 1 other connected trip
        # 00004539a0c0aee0a8723a71e445f5e265cb870ba222e51e7fde5fadc4206e33
    # print("No. of rows before connecting trips: ", df_tempview.count())
    # query = """
    # WITH data_df AS (
    #     SELECT VEHICLE_ID, DRIVER_ID, START_TIME, END_TIME, LATITUDE_PICKUP, LONGITUDE_PICKUP
    #     , LATITUDE_END, LONGITUDE_END, distance, duration_s
    #     FROM mobility_table
    #     ORDER BY VEHICLE_ID, START_TIME ASC
    # ),
    # tripPairs AS (
    #     SELECT 
    #         t1.VEHICLE_ID, 
    #         t1.DRIVER_ID,
    #         t1.START_TIME AS start_time_t1, 
    #         t1.END_TIME AS end_time_t1, t1.LATITUDE_PICKUP AS pickup_latitude_t1, 
    #         t1.LONGITUDE_PICKUP AS pickup_longitude_t1, t1.LATITUDE_END AS end_latitude_t1,
    #         t1.LONGITUDE_END AS end_longitude_t1, t1.distance AS distance_t1, t1.duration_s AS duration_s_t1,
    #         t2.START_TIME AS start_time_t2, 
    #         t2.END_TIME AS end_time_t2, t2.LATITUDE_PICKUP AS pickup_latitude_t2, 
    #         t2.LONGITUDE_PICKUP AS pickup_longitude_t2, t2.LATITUDE_END AS end_latitude_t2,
    #         t2.LONGITUDE_END AS end_longitude_t2, t2.distance AS distance_t2, t2.duration_s AS duration_s_t2
    #     FROM
    #         data_df t1
    #     JOIN
    #         data_df t2
    #     ON
    #         t1.VEHICLE_ID = t2.VEHICLE_ID
    #         AND t2.START_TIME - t1.END_TIME <= INTERVAL 1 MINUTE
    #         AND t1.END_TIME < t2.START_TIME
    #         AND t1.distance <= 6
    # ),
    # checkDoubleCount AS (
    #     SELECT *, LAG(start_time_t2, 1, '1900-01-01 00:00:00') OVER (PARTITION BY VEHICLE_ID ORDER BY start_time_t1) AS previous_start_time_t2
    #     FROM tripPairs
    # ),
    # removeDoubleCount(
    #     SELECT * FROM checkDoubleCount
    #     WHERE start_time_t1 != previous_start_time_t2 
    # ),
    # aggregatedTrips AS (
    #     SELECT
    #         VEHICLE_ID,
    #         DRIVER_ID,
    #         start_time_t1 AS START_TIME,
    #         end_time_t2 AS END_TIME,
    #         pickup_latitude_t1 AS LATITUDE_PICKUP,
    #         pickup_longitude_t1 AS LONGITUDE_PICKUP,
    #         end_latitude_t2 AS LATITUDE_END,
    #         end_longitude_t2 AS LONGITUDE_END,
    #         distance_t1 + distance_t2 AS distance,
    #         duration_s_t1 + duration_s_t2 AS duration_s
    #     FROM
    #         removeDoubleCount
    # ),
    # mergedRecords AS (
    # SELECT 
    #     at.VEHICLE_ID, 
    #     at.DRIVER_ID,
    #     at.START_TIME, 
    #     at.END_TIME, 
    #     at.LATITUDE_PICKUP, 
    #     at.LONGITUDE_PICKUP,
    #     at.LATITUDE_END, 
    #     at.LONGITUDE_END, 
    #     at.distance, 
    #     at.duration_s
    # FROM
    #     aggregatedTrips AS at

    # UNION ALL

    # SELECT
    #     d.VEHICLE_ID,
    #     d.DRIVER_ID,
    #     d.START_TIME, 
    #     d.END_TIME, 
    #     d.LATITUDE_PICKUP, 
    #     d.LONGITUDE_PICKUP,
    #     d.LATITUDE_END, 
    #     d.LONGITUDE_END, 
    #     d.distance, 
    #     d.duration_s
    # FROM
    #     data_df AS d
    # LEFT JOIN
    #     removeDoubleCount AS tp1
    # ON
    #     d.VEHICLE_ID = tp1.VEHICLE_ID
    #     AND (d.START_TIME = tp1.start_time_t1 OR d.END_TIME = tp1.end_time_t1 OR 
    #          d.START_TIME = tp1.start_time_t2 OR d.END_TIME = tp1.end_time_t2)
    # WHERE
    #     tp1.VEHICLE_ID IS NULL
    # )
    # SELECT * 
    # FROM mergedRecords
    # """
    # Execute query
    # df_tempview = spark.sql(query)
    # Replace view
    # df_tempview.createOrReplaceTempView("mobility_table")
    # print("No. of rows after connecting trips: ", df_tempview.count())

    # Step 6: Connect Trips -> ITERATIVE METHOD
    def connect_trips(partition):
        prev_vehicle = None
        prev_end_time = None
        prev_end_latitude = None
        prev_end_longitude = None
        total_duration = 0
        total_distance_km = 0
        first_trip = None
        n_connecting = 0
        connected_trips = []

        for row in partition:
            row = row.asDict()
            if prev_vehicle == row['VEHICLE_ID']:
                if (row['START_TIME'] - prev_end_time).total_seconds() <= 60 and total_distance_km < 6:
                    total_duration += row['duration_s']
                    total_distance_km += row['distance']
                    prev_end_time = row['END_TIME']
                    prev_end_latitude = row['LATITUDE_END']
                    prev_end_longitude = row['LONGITUDE_END']
                    n_connecting += 1
                else:
                    first_trip['END_TIME'] = prev_end_time
                    first_trip['LATITUDE_END'] = prev_end_latitude
                    first_trip['LONGITUDE_END'] = prev_end_longitude
                    first_trip['duration_s'] = total_duration
                    first_trip['distance'] = total_distance_km
                    connected_trips.append(first_trip)

                    first_trip = row
                    total_duration = row['duration_s']
                    total_distance_km = row['distance']
                    prev_end_time = row['END_TIME']
                    prev_end_latitude = row['LATITUDE_END']
                    prev_end_longitude = row['LONGITUDE_END']
                    n_connecting = 0
            else:
                if first_trip:
                    first_trip['END_TIME'] = prev_end_time
                    first_trip['LATITUDE_END'] = prev_end_latitude
                    first_trip['LONGITUDE_END'] = prev_end_longitude
                    first_trip['duration_s'] = total_duration
                    first_trip['distance'] = total_distance_km
                    connected_trips.append(first_trip)

                # Reset parameters
                prev_vehicle = row['VEHICLE_ID']
                first_trip = row
                total_duration = row['duration_s']
                total_distance_km = row['distance']
                prev_end_time = row['END_TIME']
                prev_end_latitude = row['LATITUDE_END']
                prev_end_longitude = row['LONGITUDE_END']
                n_connecting = 0

        if n_connecting > 0:
            first_trip['END_TIME'] = prev_end_time
            first_trip['LATITUDE_END'] = prev_end_latitude
            first_trip['LONGITUDE_END'] = prev_end_longitude
            first_trip['duration_s'] = total_duration
            first_trip['distance'] = total_distance_km
            connected_trips.append(first_trip)
        else:
            first_trip['duration_s'] = total_duration
            first_trip['distance'] = total_distance_km
            connected_trips.append(first_trip)

        return iter(connected_trips)

    # Repartition data 
    print("No. of rows before connecting trips: ", df_tempview.count())

    # 1 PARTITION METHOD
    # partitioned_df = df_tempview.coalesce(1)
    # connected_rdd = partitioned_df.rdd.mapPartitions(connect_trips)

    # REPARTITION METHOD
    partitioned_df = df_tempview.repartition("VEHICLE_ID")
    sorted_partitioned_df = partitioned_df.sortWithinPartitions("VEHICLE_ID", "START_TIME")
    connected_rdd = sorted_partitioned_df.rdd.mapPartitions(connect_trips)

    connected_df = connected_rdd.toDF(df_tempview.schema)
    print("No. of rows after connecting trips: ", connected_df.count())
    print("No. of trips removed: ", df_tempview.count() - connected_df.count())

    # Repartition back to 8 partitions if using 1 PARTITION METHOD, else comment
    # connected_df = connected_df.coalesce(8)

    connected_df.createOrReplaceTempView("mobility_table")

    
    # Step 7: Remove anomalies: Distance traveled < 1km 
    # This will be the final dataframe
    print("No. of rows before removing anomalies: ", connected_df.count())
    query = """
    SELECT *
    FROM mobility_table
    WHERE distance > 1
    """
    df_tempview = spark.sql(query)
    print("No. of rows after removing anomalies: ", df_tempview.count())
    print("No. of trips removed: ", connected_df.count() - df_tempview.count())

    # Export file to multiple csv files 
    # df_tempview.coalesce(1).write.option("header", "true").csv("results/sep2022_sample_cleaned_pyspark_repartition.csv")

    # This writes to mulitple csv file (partitions)x
    # df_tempview.write.csv("results/test", header=True, sep=',')

    # To persist Spark UI
    # time.sleep(1000000)
  
    # # Terminate Spark 
    print("Teminating spark...")
    spark.stop()

if __name__ == "__main__":
    start_time = time.time()
    main()  
    end_time = time.time()
    run_time = end_time - start_time
    print("Start time:", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)))
    print("End time:", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)))
    print("Total runtime:", run_time, "seconds")