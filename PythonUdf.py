import sys
from pyspark import Row
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import lit,col
from Pycrypto import Spark_Session
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame


from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType
from pyspark.sql.functions import date_format


def CreateDataFrame(dtf,sqlc):
    dfc = sqlc.createDataFrame(dtf)
    return dfc


def Calc(a):
    if float(a) == 8000:
        return float(a)+float(a)
    else:
        return 0

def uppercase(string):
    return string.lower()



if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext= SQLContext(conn)

chkdir =conn.setCheckpointDir("/Users/shuvamoymondal/checkpoint")


dept_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Dept.txt")
emp_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Emp.txt")

dt1=emp_rdd.map(lambda j: j.split(",")).map(lambda k: Row(empID=k[0],
                                                          Name=k[1].strip(),
                                                          Design=k[2].strip(),
                                                          Age=k[3],
                                                          Sal=k[4],
                                                          DeptID=k[5]))

dt2 =dept_rdd.map(lambda j: j.split(",")).map(lambda k: Row(
        DeptID=k[0],
        Job=k[1].strip(),
        State=k[2].strip()
    ))



EmpDF = CreateDataFrame(dt1,sql_conn)
EmpDF.show()
DeptDF = CreateDataFrame(dt2,sql_conn)
#DeptDF.show()


sql_conn.udf.register("len", lambda x: len(x), IntegerType())
sql_conn.udf.register("stringAppend", lambda x: x+'|' +x, StringType())
sql_conn.udf.register("maturity_udf",lambda S: "Doctor" if float(S)>= 5000.00 else "Teacher", StringType())
sql_conn.udf.register("Calc",Calc)
sql_conn.udf.register("uppercase", uppercase)

####Udf For dataframe ####
len_udf = udf(len, IntegerType())
EmpDF.select("Sal",len_udf("Sal")).show(5)

def scoreToCategory(Sal):
    if float(Sal) >= 8000: return 'A'
    elif float(Sal) >= 60: return 'B'
    elif float(Sal) >= 35: return 'C'
    else: return 'D'

Catg_udf = udf(scoreToCategory, StringType())
EmpDF.select("Sal",Catg_udf("Sal")).show(5)

EmpDF.createOrReplaceTempView("votes")
query = "SELECT len(Sal),maturity_udf(Sal)as Sal, Sal,stringAppend(Name),Calc(Sal),uppercase(Name) FROM votes"
df3= sql_conn.sql(query)

df3.show()
