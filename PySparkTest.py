import logging
import unittest
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

SPARK_SESSION: SparkSession = (SparkSession.builder
                               .master('local[4]')
                               .appName('pyspark test')
                               .config("spark.sql.shuffle.partitions", "20")
                               .getOrCreate())


class PySparkTest(unittest.TestCase):
    maxDiff = None

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = SPARK_SESSION

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


    @classmethod
    def df_of(cls, records: list, schema=None) -> DataFrame:
        return cls.spark.createDataFrame(records, schema=schema)

    def assertDataFrameEqual(self, expected: DataFrame, actual: DataFrame):
        self.assertCountEqual(expected.collect(), actual.collect())
        self.assertListEqual(expected.schema.names, actual.schema.names)
