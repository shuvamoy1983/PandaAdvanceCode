from pyspark import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import pandas as pd
import re
from pyspark.sql.functions import lit,col
from Pycrypto import Spark_Session
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import Row
from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Python Spark SQL basic example").master("local[*]").config("spark.some.config.option","some-value").getOrCreate()

sc= spark
conn = sc.sparkContext
sqlContext = SQLContext(conn)
dataframe_mysql = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost/mysql").option("driver","com.mysql.jdbc.Driver").option("dbtable", "test").option("user", "root").option("password", "root").load()
dataframe_mysql.show(2)

sdf_props = {'user':'root','password':'root', 'driver':'com.mysql.jdbc.Driver'}
dataframe_mysql.write.jdbc(
    url='jdbc:mysql://localhost/mysql',
    table='test',
    mode='append',
    properties = sdf_props
)