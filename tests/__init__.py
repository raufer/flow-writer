import pyspark
from pyspark import SparkConf
from flow_writer import quiet_py4j


def _default_spark_configuration():
    """Default configuration for spark's unit testing"""
    conf = SparkConf()
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "2")
    conf.set("spark.app.name", "sparkTestApp")
    return conf


def bootstrap_test_spark_session(conf=None):
    """Setup spark testing context"""
    conf = conf or _default_spark_configuration()
    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


quiet_py4j()
spark = bootstrap_test_spark_session()
