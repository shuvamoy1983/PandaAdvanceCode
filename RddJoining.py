


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


from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType
from pyspark.sql.functions import date_format
from decimal import Decimal, ROUND_HALF_UP

def CreateDataFrame(dtf,sqlc):
    dfc = sqlc.createDataFrame(dtf)
    return dfc


if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext= SQLContext(conn)
    dept_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Dept.txt")
    emp_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Emp.txt")



    print(emp_rdd.take(2))

   # print(emp_rdd.take(2))
    print(dept_rdd.take(2))
    emp = emp_rdd.map(lambda x: x.split(",")).map(lambda k: k)
    dept = dept_rdd.map(lambda x: x.split(",")).map(lambda k: k)

    m =emp.flatMap(lambda x: [(w, x) for w in x[5].split(",")])
    d= dept.flatMap(lambda x: [(w, x) for w in x[0].split(",")])
    #print(d.collect())
    data = m.join(d)
    print(data.collect())
    #print(data.map(lambda j: j[1]).collect())

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
    )
                                                )

    EmpDF = CreateDataFrame(dt1,sql_conn)
    EmpDF.printSchema()
    DeptDF = CreateDataFrame(dt2,sql_conn)
    DeptDF.show()

EmpDF.write.partitionBy("DeptID",4)

join_df= EmpDF.join(DeptDF, EmpDF.DeptID == DeptDF.DeptID,"inner").select(EmpDF.Age, EmpDF.Sal)
join_df.show(2)

EmpDF.sortWithinPartitions("deptID")
EmpDF.createOrReplaceTempView("test2")
#sql_conn.sql("SET spark.sql.shuffle.partitions = 2")
sql_conn.sql("select Sal,empID,deptid from test2 DISTRIBUTE BY deptid").show(2)
sql_conn.sql("select cast(Sal as float) NEW_SAL from test2 a union select cast(Sal as float) from test2")

EmpDF.join(DeptDF,EmpDF.DeptID == DeptDF.DeptID,'inner')\
    .select(EmpDF.Sal,F.when(EmpDF.Sal > 2850,100).otherwise(0).alias("MD_SAL")).show()

m = EmpDF.select(EmpDF.Sal.cast('float'),EmpDF.DeptID)
m.select(round(m.Sal,2)).show()



v = EmpDF.withColumn('DEPARTMENTNO',EmpDF.DeptID).withColumn('SALARY',EmpDF.Sal.cast('int'))
p = v.withColumn('Fld',lit(''))
j =v.withColumn('Fld',lit(None).cast(StringType()))
j.na.fill('50').show()


l = Window().orderBy('Fld')
q=p.select('Fld').replace('','50')
#q.withColumn('rownum',F.row_number().over(l)).show()


#v.withColumn('rownum',F.row_number().over(w)).show()


#v.orderBy(["De", "name"], ascending=[0, 1]).collect()
#EmpDF.select(EmpDF.Age,from_unixtime(unix_timestamp(EmpDF.Age,'dd-MM-yyyy'))).show()
#p=EmpDF.select(EmpDF.Age,to_date(from_unixtime(unix_timestamp(EmpDF.Age,'dd-MM-yyyy'))).alias("ModAge"))

##p.orderBy(["ModAge"],descending=[0]).show()

#w = Window().orderBy('Sal')
#l = Window().orderBy('Fld')
#q=p.select('Fld').replace('','50')
#df1=q.withColumn('row_num',F.row_number().over(l))
#df2=v.withColumn('row_num',F.row_number().over(w))

#df1.join(df2,df1.row_num==df2.row_num).show()
wSpec1 = Window.partitionBy("DeptID").orderBy("empID").rowsBetween(Window.unboundedPreceding,Window.currentRow)
r = v.select("empID","DeptID","SALARY")
r.withColumn( "movingAvg",F.sum(("SALARY")).over(wSpec1)).show()
window1 = Window.rowsBetween(-1,1)
window = Window.orderBy("SALARY")
w = Window().partitionBy("DeptID").orderBy("SALARY")
#v.select("EmpID","DeptID","SALARY",F.first("SALARY").over(window1).alias("prev")).show()

n = r.where(col("DeptID").isin({10,20}))
print(n)
