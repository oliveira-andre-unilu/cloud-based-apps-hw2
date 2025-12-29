"""
Author: Andre Martins (ID:0230991223)

Script responsible for executing the first part of the assignment (Spark analysis)
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
import time

# -----------------
# General constants
# -----------------
FILE_LOCATION : str = "./temperature.tsv"

# ------------------------
# Starting a spark session
# ------------------------

spark = SparkSession.builder.appName("TemperatureAnalysis").getOrCreate()

# ----------------
# Reading tsv file
# ----------------

# Defining the structure

schema = StructType([
    StructField("Station_id", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("measured_temperature", FloatType(), True),
    StructField("Sensor_quality", DoubleType(), True)
])

main_df = spark.read \
            .option("sep", "\t")\
            .option("header", "false")\
            .schema(schema)\
            .csv(FILE_LOCATION)


# -------------------------
# Implementing main queries
# -------------------------

start_time = time.time()

print("="*50)
print("1. For all valid information (i.e. sensor quality >= 0.95), what is the average temperature per year?")

filtered_df = main_df.filter(col("Sensor_quality") >= 0.95)

average_temperature_per_year = filtered_df.groupBy("Year").avg("measured_temperature").orderBy("Year").show()

print("="*50)
print("2. For every year in the dataset, find the station ID with the highest / lowest temperature.")

print("MAX VALUES----------------->")
# max_temperatures = main_df.groupBy("Year").max("measured_temperature").orderBy("Year").show()

max_temperatures_partition = Window.partitionBy("Year").orderBy(col("measured_temperature").desc())

max_stations = main_df\
                .withColumn("rank", row_number().over(max_temperatures_partition))\
                .filter(col("rank") == 1)\
                .select("Year", "Station_id", "measured_temperature")\
                .orderBy("Year")

max_stations.show()

# min_temperatures = main_df.groupBy("Year").min("measured_temperature").orderBy("Year").show()

print("MIN VALUES----------------->")

min_temperatures_partition = Window.partitionBy("Year").orderBy(col("measured_temperature").asc())

min_stations = main_df\
                .withColumn("rank", row_number().over(min_temperatures_partition))\
                .filter(col("rank") == 1)\
                .select("Year", "Station_id", "measured_temperature")\
                .orderBy("Year")

min_stations.show()

print("="*50)
print("3. For every year in the dataset, find the station ID with the highest maximal temperature for all stations with sensor quality >= 0.95.")

max_temperatures_partition = Window.partitionBy("Year").orderBy(col("measured_temperature").desc())

# Adding ranking for then using it for filtering

max_temperatures_per_station = filtered_df.withColumn("rank", row_number().over(max_temperatures_partition)) \
        .filter(col("rank")==1)\
        .select("Year", "Station_id", "measured_temperature")\
        .orderBy("Year")

max_temperatures_per_station.show()

print("-----------------------------------------------------------------------------------------------------")

print(f"Execution time: {(time.time()-start_time)}")

