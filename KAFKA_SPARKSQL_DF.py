from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import json
from pyspark.sql import Row, DataFrame, SQLContext
import os
import time

sc = SparkContext(appName="PythonStreamingKafkaWordCount")
ssc = StreamingContext(sc, 7)
sql = SQLContext(sc)

def process(rdd):

    try:
        # Get the singleton instance of SparkSession
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(empID=int(w[0]),Name=w[1],Designation =w[2], Age= w[3], Sal= float(w[4]),DeptID=int(w[5]) ))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("emp")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql("select * from emp")
        wordCountsDataFrame.show()
        sdf_props = {'user': 'root', 'password': 'root', 'driver': 'com.mysql.jdbc.Driver'}

        wordCountsDataFrame.write.jdbc(
            url='jdbc:mysql://localhost/mysql',
            table='emp',
            mode='append',
            properties=sdf_props
        )
    except:
        pass


zkQuorum, topic = sys.argv[1:]
print("VAL", topic)
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])
words = lines.map(lambda line: line.split(",")).map(lambda word: word).foreachRDD(process)

os.system('sh /Users/shuvamoymondal/call.sh &')
ssc.start()
ssc.awaitTermination()
