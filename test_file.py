from pyspark.sql.context import SQLContext
from Pycrypto import Spark_Session
from pyspark import Row
from pyspark.sql import functions as F
from pyspark.sql import *
import requests
if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext = SQLContext(conn)


dept_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Dept.txt")
emp_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Emp.txt")


emp = emp_rdd.map(lambda x: x.split(",")).map(lambda k: Row(empID=int(k[0]),name=k[1].strip(),Age=k[3].strip(),
                                                            Sal=float(k[4]),DeptID=int(k[5])
                                                            )
                                              )
dept = dept_rdd.map(lambda x: x.split(",")).map(lambda k: Row(DeptID=int(k[0]),State=k[2].strip()))

EmpDF=sqlContext.createDataFrame(emp)
DeptDF= sqlContext.createDataFrame(dept)
EmpDF.show()
DeptDF.show()

#emp1.join(dept1,emp1.Deptid == dept1.deptid,"inner").select(emp1.age,dept1.deptid,emp1.Sal).show(2)


EmpDF.withColumn('DEPARTMENTNO',EmpDF.DeptID).withColumn('SALARY',EmpDF.Sal).show()

EmpDF.select(EmpDF.Age,to_date(from_unixtime(unix_timestamp(EmpDF.Age,'dd-MM-yyyy'))).alias("ModAge")).show()
