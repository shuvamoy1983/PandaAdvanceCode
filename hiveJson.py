import sys
from pyspark import Row
from pyspark.sql.context import SQLContext
from Pycrypto import Spark_Session
from pyspark.sql.functions import *
from pyspark.sql.window import *
import urllib.request, json




if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext= SQLContext(conn)


df = sql_conn.read.json('/Users/shuvamoymondal/Downloads/stu.json').repartition(100)
v= df.select("name",explode("schools").alias("schools_flat"))
v.select("schools_flat.*").show()


nested_df = sql_conn.read.json(conn.parallelize(["""
{
  "field1": 1, 
  "field2": 2, 
  "nested_array":{
     "nested_field1": 3,
     "nested_field2": 4
  }
}
"""]))

flat_df = nested_df.select("field1", "field2", "nested_array.*")
flat_df.show()

df = sqlContext.read.json('/Users/shuvamoymondal/Downloads/stu.json')
df.show(1)





#df = hc.read.json(sc.parallelize(list(parse(data1))))
#df.show()