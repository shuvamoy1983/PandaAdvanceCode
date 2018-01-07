from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext


class Spark_Session:
    def Spark_Connect(self):
        spark = SparkSession.builder.appName("Python Spark SQL basic example").master("local[*]").config("spark.some.config.option",
                                                                                      "some-value").getOrCreate()
        #spark.conf.set("spark.sql.shuffle.partitions", 6)
        #spark.conf.set("spark.executor.memory", "1g")
        return spark

    def Spark_Context(self):
        sprk = Spark_Session()
        conn = sprk.Spark_Connect()
        sc = conn.sparkContext
        return sc

    def spark_conf(self):
        s_conf = SparkConf()
        return s_conf
