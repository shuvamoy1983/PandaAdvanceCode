from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import json


sc = SparkContext(appName="PythonStreamingKafkaWordCount")
ssc = StreamingContext(sc, 3)

zkQuorum, topic = sys.argv[1:]
print("VAL",topic)
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])
lines.pprint()

counts = lines.flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
counts.pprint()

ssc.start()
ssc.awaitTermination()
