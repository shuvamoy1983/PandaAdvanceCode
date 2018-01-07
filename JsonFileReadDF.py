import sys
from pyspark import Row
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import lit,col
from Pycrypto import Spark_Session
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *
from pyspark.sql.window import *
import urllib.request, json
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
import requests
import wget

def parse(text):
    try:
        return json.loads(text)
    except ValueError as e:
        print('invalid json: %s' % e)
        return None # or: raise

def convert_to_line(json_list):
    json_string = ""
    for line in json_list:
        json_string += json.dumps(line) + "\n"
        print(json_string)
    return json_string


def parse_json(json_data,sc):
    r = convert_to_line(json_data)
    mylist = []
    for line in r.splitlines():
        mylist.append(line)
    rdd = sc.parallelize(mylist,8)
    df = sqlContext.read.json(rdd)
    return df


if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext = SQLContext(conn)


##https://api.github.com/users?since=100
with urllib.request.urlopen("https://api.github.com/users?since=100") as url:
    data = parse_json(parse(url.read().decode("utf-8")),conn)

data.show()





