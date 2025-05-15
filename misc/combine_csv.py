# This Python file is to combine monthly CSVs into a single CSV
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Combine CSV Files").getOrCreate()

file_pattern = ""

combined_df = spark.read.option("header", "true").csv(file_pattern)

combined_df.coalesce(1).write.option("header", "true").csv("s3a://fyp-ken/raw_combine_data/Jan_to_Feb_2022")
