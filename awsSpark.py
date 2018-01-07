
from pyspark import Row
import boto3
from pyspark.sql.context import SQLContext
from Pycrypto import Spark_Session


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

    #conn._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", 'AKIAIY7NU6XS3QCQLHJQ')
    #conn._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", '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')
    #sql_conn.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJU5JMSYYA3ZWVH3A")
    #sql_conn.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")


    s3 = boto3.resource('s3')
   # s3.create_bucket(Bucket='sumittest444', CreateBucketConfiguration={
    #    'LocationConstraint': 'us-west-2'})

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



EmpDF.write.parquet("s3n://test56569/test9.txt")

sql_conn.read.parquet("s3n://test56569/test9.txt").show(3)
