from os import replace

from pyspark import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import pandas as pd
import re
from pyspark.sql.functions import *
from Pycrypto import Spark_Session

if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    lines = conn.textFile("/Users/shuvamoymondal/Downloads/Consumer_Complaints.csv")
    print(lines.count())
    parts = lines.map(lambda l: l.split(","))
    p = parts.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda y: y[0])

    data = p.map(lambda h: Row(re.sub('"','',h[0]).strip(),re.sub('"','',h[1])
                               )).toDF()

    data1 = p.map(
        lambda h: Row(re.sub('"', '', h[0]).strip(), re.sub('"', '', h[1]),
                      )).toDF()
    print(data.show(10))
    print(data1.show(10))
