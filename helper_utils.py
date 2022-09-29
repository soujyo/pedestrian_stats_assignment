"""
Module to use all the helper functions
"""
import configparser

from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def load_data_frame(spark, schema, file_path):
    spark.read.option("header", True).schema(schema).csv(file_path)


def load_config():
    """
    Loads the configs from config file
    """
    config = configparser.RawConfigParser()
    config.read('config.cfg')
    return config


def check_nulls(df: DataFrame):
    has_null_flag = False
    null_col_list = \
        df.select([count(when(col(c).isNull(), c).when(col(c) == '', c)).alias(c) for c in df.columns]).collect()[0]
    print(null_col_list)
    for i in null_col_list:
        if i != 0:
            has_null_flag = True
    return has_null_flag


def check_dataframe_isEmpty(df: DataFrame):
    return df.rdd.isEmpty()


def check_dataframe_isEmpty(df: DataFrame):
    return df.rdd.isEmpty()
