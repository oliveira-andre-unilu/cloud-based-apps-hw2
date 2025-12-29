"""
Author: Andre Martins (ID:0230991223)

Script responsible for executing the second part of the assignment (MongoDB analysis)
"""
from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
import time

# ----------------
# Helper functions
# ----------------

def pretty_print_results(results, title="Query Results"):
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    
    for i, doc in enumerate(results, 1):
        print(f"\nResult {i}:")
        for key, value in doc.items():
            if isinstance(value, float):
                print(f"  {key:20}: {value:.2f}")
            else:
                print(f"  {key:20}: {value}")

# -----------------
# General constants
# -----------------
FILE_LOCATION : str = "./temperature.tsv"
start_time = time.time()

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

# ----------------------------
# Loading data into mongodb db
# ----------------------------

pandas_df = main_df.toPandas()

# ----------------------------------------------------
# Connecting to mongodb and deleting initializing data
# ----------------------------------------------------

mongo_client = MongoClient(
    host="mongodb",
    port=27017,
    username='cloud-user',
    password='LetMeIn1234',
)
mongo_db = mongo_client["clound_based_apps"]
mongo_collection = mongo_db["temperature_readings"]

mongo_collection.delete_many({}) # Clearing existing data

mongo_collection.insert_many(pandas_df.to_dict('records')) # Adding all the data

mongo_collection.create_index([("Year", 1)])
mongo_collection.create_index([("Sensor_quality", 1)])
mongo_collection.create_index([("Year", 1), ("measured_temperature", -1)])

# ---------------------
# Executing all queries
# ---------------------

start_time = time.time()

# 1. For all valid information (i.e. sensor quality >= 0.95), what is the average temperature per year?

pipeline = [
    {"$match": {"Sensor_quality": {"$gte": 0.95}}},
    {"$group": {
        "_id": "$Year",
        "avg_temperature": {"$avg": "$measured_temperature"}
    }},
    {"$sort": {"_id": 1}}
]

try:
    results = list(mongo_collection.aggregate(pipeline))
    pretty_print_results(results)
except Exception as e:
    print(f"Error executing pipeline: {e}")

# 2. For every year in the dataset, find the station ID with the highest / lowest temperature.

pipeline_highest = [
    {"$sort": {"Year" : 1, "measured_temperature": -1}},
    {"$group": {
        "_id": "$Year",
        "max_temperature_station_id": {"$first": "$Station_id"},
        "max_temperature": {"$first": "$measured_temperature"},
        "min_temperature_station_id": {"$last": "$Station_id"},
        "min_temperature": {"$last": "$measured_temperature"}
    }},
    {"$sort": {"_id": 1}}
]


try:
    results = list(mongo_collection.aggregate(pipeline_highest))
    pretty_print_results(results)
except Exception as e:
    print(f"Error executing pipeline: {e}")


# 3. For every year in the dataset, find the station ID with the highest maximal temperature for all stations with sensor quality >= 0.95.

pipeline_max_temp_per_station = [
    {"$match": {"Sensor_quality": {"$gte": 0.95}}},
    {"$sort": {"Year" : 1, "measured_temperature": -1}},
    {"$group": {
        "_id": "$Year",
        "Station_id": {"$first": "$Station_id"},
        "max_temperature": {"$first": "$measured_temperature"}
    }}
]

try:
    results = list(mongo_collection.aggregate(pipeline_max_temp_per_station))
    pretty_print_results(results)
    
except Exception as e:
    print(f"Error executing pipeline: {e}")


print("-----------------------------------------------------------------------------------------------------")

print(f"Execution time: {(time.time()-start_time)}")


