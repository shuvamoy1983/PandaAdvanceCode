
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
    sqlContext=SQLContext(conn)
    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema",
                                                                                            "true")\
        .option("delimiter",",").load("/Users/shuvamoymondal/Downloads/SalesJan2009.csv")
    df.show(2)

v = sqlContext.read.json("/Users/shuvamoymondal/Downloads/test2.json", multiLine=True)


v.printSchema()