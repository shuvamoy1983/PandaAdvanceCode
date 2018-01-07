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
    spark = SparkSession.builder.appName("Python Spark SQL basic example").master("local[*]").config(
        "spark.some.config.option"
        "some-value").getOrCreate()

    sc= spark
    conn = sc.sparkContext
    sqlContext = SQLContext(conn)

    dept_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Dept.txt")
    emp_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Emp.txt")
    emp = emp_rdd.map(lambda x: x.split(",")).map(lambda k: k)
    dept = dept_rdd.map(lambda x: x.split(",")).map(lambda k: k)
    dt1 = emp_rdd.map(lambda j: j.split(",")).map(lambda k: Row(
        empID=k[0],
        Name=k[1].strip(),
        Design=k[2].strip(),
        Age=k[3],
        Sal=k[4],
        DeptID=int(k[5])
    ))

    dt2 = dept_rdd.map(lambda j: j.split(",")).map(lambda k: Row(
        DeptID=int(k[0]),
        Job=k[1].strip(),
        State=k[2].strip()))

    EmpDF = sqlContext.createDataFrame(dt1)
    DeptDF = sqlContext.createDataFrame(dt2)






