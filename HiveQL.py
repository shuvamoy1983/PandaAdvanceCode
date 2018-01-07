from pyspark.sql import SparkSession
from pyspark.sql import Row
from os.path import expanduser, join, abspath
from Pycrypto import Spark_Session
from pyspark.sql.context import SQLContext
from pyspark.sql import HiveContext
# warehouse_location points to the default location for managed databases and tables

if __name__ == '__main__':
    warehouse_location = abspath('Users\shuvamoymondal\spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
hc = HiveContext(sc)

hc.sql("CREATE TABLE IF NOT EXISTS employee (Eid Int, Name String,Designation String,"
       "Age String,Salary String,DeptId Int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")

hc.sql("LOAD DATA LOCAL INPATH '/Users/shuvamoymondal/Downloads/Emp.txt' INTO TABLE employee ")

hc.sql("select eid, sum(salary) from employee group by eid").show()