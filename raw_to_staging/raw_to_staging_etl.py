"""
Pyspark ETL to load csv data files from raw zone and transform them to place in the staging zone
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from helper_utils import *

config_obj = load_config()

RAW_PATH_MONTHLY_COUNT_DATA = config_obj['DEFAULT']['monthly_counts_raw_path']
RAW_PATH_SENSOR_DIM_DATA = config_obj['DEFAULT']['sensor_location_raw_path']
STAGE_PATH_MONTHLY_COUNT_DATA = config_obj['DEFAULT']['monthly_counts_stage_path']
STAGE_PATH_SENSOR_DIM_DATA = config_obj['DEFAULT']['sensor_location_stage_path']

# Create spark session
spark = SparkSession.builder.appName('Raw to Staging data load').getOrCreate()
"""Define the schema for the raw datasets"""
monthly_counts_schema = StructType([StructField("ID", IntegerType(), False) \
                                       , StructField("Date_Time", StringType(), True) \
                                       , StructField("Year", IntegerType(), True) \
                                       , StructField("Month", StringType(), True) \
                                       , StructField("Mdate", IntegerType(), True) \
                                       , StructField("Day", StringType(), True) \
                                       , StructField("Time", IntegerType(), True) \
                                       , StructField("Sensor_ID", IntegerType(), True) \
                                       , StructField("Sensor_Name", StringType(), True) \
                                       , StructField("Hourly_Counts", IntegerType(), True)])

sensor_dims_schema = StructType([StructField("sensor_id", IntegerType(), False) \
                                    , StructField("sensor_description", StringType(), True) \
                                    , StructField("sensor_name", StringType(), True) \
                                    , StructField("installation_date", StringType(), True) \
                                    , StructField("status", StringType(), True) \
                                    , StructField("note", StringType(), True) \
                                    , StructField("direction_1", StringType(), True) \
                                    , StructField("direction_2", StringType(), True) \
                                    , StructField("latitude", StringType(), True) \
                                    , StructField("longitude", StringType(), True) \
                                    , StructField("location", StringType(), True)])

df_monthly_counts = load_data_frame(spark, monthly_counts_schema, RAW_PATH_MONTHLY_COUNT_DATA)
df_sensor_dims = load_data_frame(spark, sensor_dims_schema, RAW_PATH_SENSOR_DIM_DATA)

"""Test datasets for empty data records"""
print(f"No null records found in  sensor dimensions data:{check_nulls(df_sensor_dims)}")
print(f"No null records found in  sensor dimensions data:{check_nulls(df_monthly_counts)}")
# Test datasets for null values in any columns
print(f"Null values has been found in the columns for sensor dimensions data:{check_nulls(df_sensor_dims)}")
print(f"Null values has been found in the columns for monthly counts data:{check_nulls(df_monthly_counts)}")

"""Filtering columns of interest"""
df_sensor_dims = df_sensor_dims.select(col("sensor_id"), col("status"), col("latitude"), col("longitude"),
                                       col("location"))
"""Adding load_date_time and load source as an audit column for later audits.Hash of row is calculated to identify each unique row for incremental updates later if required"""
df_monthly_counts_final = df_monthly_counts.withColumn("ingestion_date", current_timestamp()).withColumn(
    "ingestion_source",
    lit("api")).withColumn("row_hash", sha2(concat_ws("||", *df_monthly_counts.columns), 256))
df_sensor_dims_final = df_sensor_dims.withColumn("ingestion_date", current_timestamp()).withColumn("ingestion_source",
                                                                                                   lit("sftp"))

"""Saving the datasets in stage zone in parquet format for optimized processing in spark"""
# Partition by month and year with overwrite mode helps to overwrite any partitions during reprocessing of a particular monthly load as data is loaded in monthly frequency
df_monthly_counts_final.write.mode('overwrite').partitionBy("Month", "Year").parquet(STAGE_PATH_MONTHLY_COUNT_DATA)

# Sensor dimension table doesn't change frequently and partitioning is not required as data set will be small
df_sensor_dims_final.write.mode('overwrite').parquet(STAGE_PATH_SENSOR_DIM_DATA)
