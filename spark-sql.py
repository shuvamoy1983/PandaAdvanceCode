import sys
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

def CreateDataFrame(dtf,sqlc):
    dfc = sqlc.createDataFrame(dtf)
    return dfc

if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn = sprk.Spark_Connect()
    sqlContext = SQLContext(conn)
    emp_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Emp.txt")
    dept_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Dept.txt")


dt1 = emp_rdd.map(lambda j: j.split(",")).map(lambda k: Row(
    empID=k[0],
    Name=k[1].strip(),
    Design=k[2].strip(),
    Age=k[3],
    Sal=k[4],
    DeptID=int(k[5])
))

dt2 = dept_rdd.map(lambda j: j.split(",")).map(lambda k: Row(
    DeptID=k[0],
    Job=k[1].strip(),
    State=k[2].strip()
)
                                               )

EmpDF = CreateDataFrame(dt1, sql_conn)
EmpDF.show()
DeptDF = CreateDataFrame(dt2, sql_conn)
DeptDF.show()

EmpDF.createOrReplaceTempView("emp")
DeptDF.createOrReplaceTempView("dept")

sql_conn.sql("select e.Sal, e.deptId, e.empID from emp e where exists(select * from dept d where d.deptId=e.deptID and d.deptID=10 )")
