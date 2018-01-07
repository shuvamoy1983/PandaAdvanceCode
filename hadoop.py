from pyparsing import col
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def replace_character(str):
    for i in str:
        return i.replace("\"", '')


def mapper(line):
    field = line.split(',')
    return Row(first_name=field[0], last_name=str(field[1]),
               company_name=str(field[2]),address=str(field[3]),city=str(field[4]),county=str(field[5]),
    state = str(field[6]),zip=str(field[7]),phone1=str(field[8]),phone2=str(field[9]),
               email=str(field[10]), web=str(field[11]))


data = spark.read.text("hdfs://localhost:9000/user/shuva/us-500.csv")
header = data.first()

schemaString = replace_character(header)

fields = [StructField(field_name, StringType(), True)
          for field_name in schemaString.split(',')]

schema = StructType(fields)
lines = spark.sparkContext.textFile("hdfs://localhost:9000/user/shuva/us-500.csv")
df = lines.map(mapper)
schemaDf = spark.createDataFrame(df).cache()

j = schemaDf.filter(udf(lambda target: target.startswith("first_name"), BooleanType())(schemaDf.target))
print(j)
schemaDf.createOrReplaceTempView("mytab")
