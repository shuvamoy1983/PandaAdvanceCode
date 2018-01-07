from pyspark import Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
from Pycrypto import Spark_Session
from pyspark.sql.types import *

if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn =sprk.Spark_Connect()
    dept_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Dept.txt")
    ## Create RDD from file convert to line to a Row
    emp_rdd = conn.textFile("/Users/shuvamoymondal/Downloads/Emp.txt").map(lambda s: s.split(","))
    print(emp_rdd.take(3))
    # Each line is converted to a tuple.
    emp = emp_rdd.map(lambda s: (s[0],s[1].strip(),s[2].strip(),s[3].strip(),s[4],s[5]))
    print(emp.take(2))


# The schema is encoded in a string.
schemaString = "Id Name Desgn Age Sal Dept_ID"

## Now read all the field defined above as String type and crete StructField
## Next bind all the field to type
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
df = sql_conn.createDataFrame(emp,schema)
df.show()
df.createOrReplaceTempView("emp")
sqlContext.sql("select sum(")

sqlContext=SQLContext(conn)
data = sqlContext.load(source="com.databricks.spark.csv", path = '/Users/shuvamoymondal/Downloads/SalesJan2009', header = True,inferSchema = True)
data.show(2)