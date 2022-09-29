"""
Pyspark ETL to load data from stage zone and transform them to place in the aggregated zone
"""
from pyspark.sql import SparkSession

from helper_utils import *

config_obj = load_config()

STAGE_PATH_MONTHLY_COUNT_DATA = config_obj['DEFAULT']['monthly_counts_stage_path']
STAGE_PATH_SENSOR_DIM_DATA = config_obj['DEFAULT']['sensor_location_stage_path']
STAGE_FILE_FORMAT = config_obj['DEFAULT']['stage_file_format']
AGGREGATED_DB_NAME = config_obj['DEFAULT']['aggregated_db_name']
ALL_COUNT_LOCATION_TABLE = config_obj['DEFAULT']['all_count_location_table']
COUNT_BY_LOCATION_DAY_AGG_TABLE = config_obj['DEFAULT']['all_count_location_day_agg_table']
COUNT_BY_LOCATION_MONTH_AGG_TABLE = config_obj['DEFAULT']['all_count_location_month_agg_table']
COUNT_BY_LOCATION_YEAR_AGG_TABLE = config_obj['DEFAULT']['all_count_location_year_agg_table']

#Create spark session
spark = SparkSession.builder.appName('Stage to Aggregated data load').getOrCreate()

"""Load data from the stage zone"""
df_monthly_counts_stage = spark.read.parquet(STAGE_PATH_MONTHLY_COUNT_DATA)
df_sensor_dims_stage = spark.read.parquet(STAGE_PATH_SENSOR_DIM_DATA)

"""Merge the monthly count dataset with the sensor location data to enrich data with sensor location"""
merged_df = df_monthly_counts_stage.join(df_sensor_dims_stage,
                                         df_monthly_counts_stage.Sensor_ID == df_sensor_dims_stage.sensor_id, "inner") \
    .select(col("Date_Time"), col("Year"), col("Month"), col("Day"), col("Mdate"), col("Time"), col("location"),
            col("Hourly_Counts"))
merged_df.cache()  # to be reused later
"""Derive aggregated dimensions of location and day"""
pedestrian_count_per_loc_per_day = merged_df.groupBy("Day", "location").agg({"Hourly_Counts": "sum"})
"""Derive aggregated dimensions of location and month"""
pedestrian_count_per_loc_per_month = merged_df.groupBy("Month", "location").agg({"Hourly_Counts": "sum"})
"""Derive aggregated dimensions of location and Year"""
pedestrian_count_per_loc_per_year = merged_df.groupBy("Year", "location").agg({"Hourly_Counts": "sum"})



"""Saving the datasets in aggregated zone as tables for data warehouse"""
"""These tables can be directly saved into the Snowflake data warehouse which aids further analysis"""
# Partition by month and year with overwrite mode helps to overwrite any partitions during reprocessing of a particular monthly load as data is loaded in monthly frequency
merged_df.mode("overwrite").partitionBy("Month", "Year").format(STAGE_FILE_FORMAT).saveAsTable(
    f"{AGGREGATED_DB_NAME}.{ALL_COUNT_LOCATION_TABLE}")

merged_df.unpersist()  # evict from memory

pedestrian_count_per_loc_per_day.mode("overwrite").partitionBy("Day").format(STAGE_FILE_FORMAT).saveAsTable(
    f"{AGGREGATED_DB_NAME}.{COUNT_BY_LOCATION_DAY_AGG_TABLE}")
pedestrian_count_per_loc_per_month.mode("overwrite").partitionBy("Month").format(STAGE_FILE_FORMAT).saveAsTable(
    f"{AGGREGATED_DB_NAME}.{COUNT_BY_LOCATION_DAY_AGG_TABLE}")
pedestrian_count_per_loc_per_year.mode("overwrite").partitionBy("Year").format(STAGE_FILE_FORMAT).saveAsTable(
    f"{AGGREGATED_DB_NAME}.{COUNT_BY_LOCATION_DAY_AGG_TABLE}")

