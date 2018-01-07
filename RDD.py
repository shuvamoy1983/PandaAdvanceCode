

from pyspark import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import pandas as pd
import re
from pyspark.sql.functions import *
from operator import add
from Pycrypto import Spark_Session

def sum_cal(v):
    sum = 0.0
    for i in v:
        sum +=i
    return sum

if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    lines = conn.textFile("/Users/shuvamoymondal/Downloads/emp.txt").map(lambda v: v.split(",")).map(lambda g: (g[5],float(g[4])))

print(lines.collect())
v= lines.map(lambda h: h[1]).max()
print(v)
