from pyspark import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import pandas as pd
import re

from Pycrypto import Spark_Session

if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    lines = conn.textFile("/Users/shuvamoymondal/Downloads/SalesJan2009.csv")
    print(lines.count())
    parts = lines.map(lambda l: l.split(","))
    p = parts.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda y: y[0])
    data = p.map(lambda h: Row(h[0],h[1],h[2],h[3]))
    print(data.take(2))

