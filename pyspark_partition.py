import sys
from pyspark import Row
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
import pandas as pd
import re
from pyspark.sql.functions import lit,col
from Pycrypto import Spark_Session
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.rdd import portable_hash

def CreateDataFrame(dtf,sqlc):
    dfc = sqlc.createDataFrame(dtf)
    return dfc


if __name__ == '__main__':
    sprk = Spark_Session()
    sc= sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext= SQLContext(sc)
    nums = range(0, 10)
    #emp_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Dept.txt",10)
    #emp_rdd = sc.textFile("/Users/shuvamoymondal/Downloads/Emp.txt")



#print("Record:" ,emp_rdd.count())
#print("Default parallelism: {}".format(conn.defaultParallelism))
#print("Number of partitions: {}".format(emp_rdd.getNumPartitions()))
#print("Partitioner: {}".format(emp_rdd.partitioner))
#print("Partitions structure: {}".format(emp_rdd.glom().collect()))
#EmpDF = CreateDataFrame(dt1,sql_conn)
#EmpDF.printSchema()
#DeptDF = CreateDataFrame(dt2,sql_conn)
#DeptDF.show()

def empid_partitioner(ID):
    print("The partition number is ",hash(ID) % 4)
    return hash(ID)

emp_rdd = sc.textFile("/Users/shuvamoymondal/Downloads/Emp.txt")
dept_rdd = sc.textFile("/Users/shuvamoymondal/Downloads/Dept.txt")

b = emp_rdd.map(lambda j: j.split(",")).map(lambda k:k).partitionBy(empid_partitioner)
dt1=emp_rdd.map(lambda j: j.split(",")).map(lambda k: Row(
        empID=k[0],
        Name=k[1].strip(),
        Design=k[2].strip(),
        Age=k[3],
        Sal=k[4],
        DeptID=int(k[5])
    ))


dt2 =dept_rdd.map(lambda j: j.split(",")).map(lambda k: Row(
        DeptID=k[0],
        Job=k[1].strip(),
        State=k[2].strip()
    ))

EmpDF =CreateDataFrame(dt1, sql_conn)
DeptDF = CreateDataFrame(dt2, sql_conn)
DeptDF.show()


##EmpDF.partitionBy(4,empid_partitioner)
