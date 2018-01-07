from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import StringType


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

data = spark.read.text("hdfs://localhost:9000/user/shuva/us-500.csv")
print(data.take(2))
header = data.first()
taxiNoHeader = data.show