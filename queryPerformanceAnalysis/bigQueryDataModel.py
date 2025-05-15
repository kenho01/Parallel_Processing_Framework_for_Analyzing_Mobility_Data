from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, dayofmonth, month, year, date_format, monotonically_increasing_id, to_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F

conf = SparkConf().setAppName("Fact Dimension Data Model").setMaster("local[8]").set("spark.driver.memory", "4g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.csv("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/results/Feb2022_cleaned.csv", header=True, inferSchema=True)

# df.printSchema()
# print(spark.conf.get("spark.sql.session.timeZone"))

# DT_START and DT_END are of timestamp data type

#Feb2022_cleaned 

# Dim_Vehicle
dim_vehicle = df.select("VEHICLE_ID").distinct()

# Dim_Driver
dim_driver = df.select("DRIVER_ID").distinct()

# Dim_Pickup_Location
dim_pickup_location = df.select(
    F.col("LATITUDE_PICKUP").alias("LATITUDE_PICKUP"),
    F.col("LONGITUDE_PICKUP").alias("LONGITUDE_PICKUP")
).distinct().withColumn(
    "PICKUP_LOCATION_ID",
    row_number().over(Window.orderBy("LATITUDE_PICKUP", "LONGITUDE_PICKUP"))
)

# Dim_Dropoff_Location
dim_dropoff_location = df.select(
    F.col("LATITUDE_END").alias("LATITUDE_DROPOFF"),
    F.col("LONGITUDE_END").alias("LONGITUDE_DROPOFF")
).distinct().withColumn(
    "DROPOFF_LOCATION_ID",
    row_number().over(Window.orderBy("LATITUDE_DROPOFF", "LONGITUDE_DROPOFF"))
)

# Dim_Pickup_Time
dim_pickup_time = df.select(
    F.col("DT_START").alias("PICKUP_DT"),
    year("DT_START").alias("year"),
    month("DT_START").alias("month"),
    dayofmonth("DT_START").alias("day")
).distinct().withColumn(
    "PICKUP_TIME_ID",
    row_number().over(Window.orderBy("PICKUP_DT"))
)

# Dim_Dropoff_Time
dim_dropoff_time = df.select(
    F.col("DT_END").alias("DROPOFF_DT"),
    year("DT_END").alias("year"),
    month("DT_END").alias("month"),
    dayofmonth("DT_END").alias("day")
).distinct().withColumn(
    "DROPOFF_TIME_ID",
    row_number().over(Window.orderBy("DROPOFF_DT"))
)

# Fact_Trips
df = df.withColumn("TRIP_ID", F.monotonically_increasing_id())

# Join Fact with Dim_Pickup_Location
df = df.join(
    dim_pickup_location,
    (df["LATITUDE_PICKUP"] == dim_pickup_location["LATITUDE_PICKUP"]) &
    (df["LONGITUDE_PICKUP"] == dim_pickup_location["LONGITUDE_PICKUP"]),
    "left"
)

# Join Fact with Dim_Dropoff_Location
df = df.join(
    dim_dropoff_location,
    (df["LATITUDE_END"] == dim_dropoff_location["LATITUDE_DROPOFF"]) &
    (df["LONGITUDE_END"] == dim_dropoff_location["LONGITUDE_DROPOFF"]),
    "left"
)

# Join Fact with Dim_Pickup_Time
df = df.join(
    dim_pickup_time, 
    (df["DT_START"] == dim_pickup_time["PICKUP_DT"]),
    "left"
)

# Join Fact with Dim_Dropoff_Time
df = df.join(
    dim_dropoff_time, 
    (df["DT_END"] == dim_dropoff_time["DROPOFF_DT"]),
    "left"
)

# Select final Fact_Trips table
fact_trips = df.select(
    "TRIP_ID", "REQUEST_ID", "VEHICLE_ID", "DRIVER_ID", 
    "duration_s", "distance_km",
    "PICKUP_TIME_ID", "DROPOFF_TIME_ID",
    "PICKUP_LOCATION_ID", "DROPOFF_LOCATION_ID"
)

# Save Tables
dim_vehicle.write.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_vehicle.parquet", mode="overwrite")
dim_driver.write.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_driver.parquet", mode="overwrite")
dim_pickup_time.write.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_pickup_time.parquet", mode="overwrite")
dim_dropoff_time.write.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_dropoff_time.parquet", mode="overwrite")
dim_pickup_location.write.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_pickup_location.parquet", mode="overwrite")
dim_dropoff_location.write.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/dim_dropoff_location.parquet", mode="overwrite")
fact_trips.write.parquet("/Users/kenho/Documents/GitHub/AY2425_Ken_Process-Optimisation/dataWarehouse/fact_trips.parquet", mode="overwrite")

# Stop Spark session
spark.stop()
