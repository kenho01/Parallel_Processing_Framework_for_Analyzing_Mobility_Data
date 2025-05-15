# This is the main python file to perform data processing with PySpark (CLUSTER)

# Import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pandas as pd
from pyspark.sql.functions import udf, to_timestamp, regexp_extract
from pyspark.sql.types import DoubleType
from shapely.geometry import shape, Point
from pyspark.sql.types import IntegerType
import geopandas as gpd
import json
import os
import sys
import time
import math
import fsspec

def main():
    # Start time
    start_time = time.time()

    # Pass credentials through .env file
    fs = fsspec.filesystem(
    's3', 
    key=os.getenv("MINIO_KEY"), 
    secret=os.getenv("MINIO_SECERT"), 
    client_kwargs={'endpoint_url': 'http://minio.ntuhpc.org:9000'}
    )

    # Initialize & Configure Spark session
    conf = SparkConf().setAppName("Mobility Data Process Optimisation (Jan 2022)")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    conf = spark.sparkContext.getConf()

    file_path = "s3a://fyp-ken/Jan2022.csv"
    raw_rdd = spark.sparkContext.textFile(file_path)

    # Remove trips with more than 9 columns
    raw_filtered_rdd = raw_rdd.filter(lambda row: len(row.split(",")) == 9)
    columns = ["VEHICLE_ID", "DRIVER_ID", "REQUEST_ID", "DT_START", "DT_END", "LATITUDE_PICKUP", "LONGITUDE_PICKUP", "LATITUDE_END", "LONGITUDE_END"]
    raw_filtered_df = raw_filtered_rdd.map(lambda row: row.split(",")).toDF(columns)

    # Remove trips with invalid DT_START and DT_END expression
    timestamp_format = "dd/MM/yyyy HH:mm:ss"
    valid_timestamp_regex = r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"

    raw_filtered_df = raw_filtered_df.filter((regexp_extract("DT_START", valid_timestamp_regex, 0) != "") &
                (regexp_extract("DT_END", valid_timestamp_regex, 0) != ""))

    raw_filtered_df = raw_filtered_df.withColumn("DT_START", to_timestamp("DT_START", timestamp_format)) \
        .withColumn("DT_END", to_timestamp("DT_END", timestamp_format))

    # print("AFTER: ", raw_filtered_df.count())

    raw_filtered_df.createOrReplaceTempView("mobility_table")

    # Read geojson file
    with fs.open('fyp-ken/MasterPlan2019PlanningAreaBoundaryNoSea.geojson') as file:
        gdf = gpd.read_file(file)
    polygons = [shape(feature["geometry"]) for feature in json.loads(gdf.to_json())["features"]]  

    # User defined function: returns 1 if given latitude & longitude is within SG's boundary, else return 0. If None, return None
    @udf(returnType=IntegerType())
    def within_boundary(latitude, longitude):
        if latitude is None or longitude is None:
            return None
        point = Point(longitude, latitude)
        return 1 if any(polygon.contains(point) for polygon in polygons) else 0
    spark.udf.register("within_boundary", within_boundary)

    # START OF DATA CLEANING
    # Step 1: Cast datetime from string to timestamp, latitude & longitude to float, call UDF to check for off-boundary points, removing NULL latitudes & longitudes
    query = """
    SELECT 
        VEHICLE_ID, REQUEST_ID, DRIVER_ID, CAST(LATITUDE_PICKUP AS NUMERIC(11,8)) AS LATITUDE_PICKUP, CAST(LONGITUDE_PICKUP AS NUMERIC(11,8)) AS LONGITUDE_PICKUP
        ,CAST(LATITUDE_END AS NUMERIC(11,8)) AS LATITUDE_END, CAST(LONGITUDE_END AS NUMERIC(11,8)) AS LONGITUDE_END, 
        to_timestamp(DT_START,'dd/MM/yyyy HH:mm:ss') AS START_TIME, to_timestamp(DT_END,'dd/MM/yyyy HH:mm:ss') AS END_TIME,
        within_boundary(CAST(LATITUDE_PICKUP AS NUMERIC(11,8)), CAST(LONGITUDE_PICKUP AS NUMERIC(11,8))) AS WITHIN_BOUNDARY_PICKUP,
        within_boundary(CAST(LATITUDE_END AS NUMERIC(11,8)), CAST(LONGITUDE_END AS NUMERIC(11,8))) AS WITHIN_BOUNDARY_END
    FROM 
        mobility_table
    WHERE
        to_timestamp(DT_START, 'dd/MM/yyyy HH:mm:ss') > '1970-01-01 00:00:00'
    AND 
        to_timestamp(DT_END, 'dd/MM/yyyy HH:mm:ss') > '1970-01-01 00:00:00'
    """
    # Execute query
    df_tempview = spark.sql(query)
    df_tempview.createOrReplaceTempView("mobility_table")

    # Step 2: Remove off-boundary points 
    query = f"""
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

    # Step 3: Remove duplicates
        # 1. VEHICLE_ID is with 1 DRIVER_ID at a time
        # 2. Any trips that start before the previous trip end using the same VEHICLE_ID will be dropped
    # Repartition data by VEHICLE_ID to ensure all trips for a vehicle are in the same partition
    df_tempview = df_tempview.repartition("VEHICLE_ID")
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

    # START OF DATA PROCESSING

    # Step 4: Calculate distance travelled and time taken for each trip 
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
    
    # Step 5: Connect Trips -> ITERATIVE METHOD
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

    # REPARTITION METHOD
    partitioned_df = df_tempview.repartition("VEHICLE_ID")
    sorted_partitioned_df = partitioned_df.sortWithinPartitions("VEHICLE_ID", "START_TIME")
    connected_rdd = sorted_partitioned_df.rdd.mapPartitions(connect_trips)
    connected_df = connected_rdd.toDF(df_tempview.schema)
    connected_df.createOrReplaceTempView("mobility_table")

    # Step 6: Remove anomalies: Distance traveled < 1km 
    # This will be the final dataframe
    query = """
    SELECT *
    FROM mobility_table
    WHERE distance > 1
    """
    df_tempview = spark.sql(query)
    print("FINAL DF SIZE:", df_tempview.count())

    # Check size of output dataframe, comment if not needed
    # print("After: ", df_tempview.count())

    # Export file to 1 csv file
    # df_tempview.coalesce(1).write.option("header", "true").csv("s3a://fyp-ken/results_combined/Jan2022")

    # This writes to mulitple csv file (based on # of partitions)
    # df_tempview.write.csv("s3a://fyp-ken/results_partitioned/Jan2022", header=True, sep=',')

    end_time = time.time()
    run_time = end_time - start_time
    print("Start time:", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)))
    print("End time:", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)))
    print("Total runtime:", run_time, "seconds")

    # To persist Spark UI
    # time.sleep(1000000)
  
    # # Terminate Spark 
    print("Teminating spark...")
    spark.stop()
    
if __name__ == "__main__":
    main()