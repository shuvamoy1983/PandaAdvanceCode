
import sys
from pyspark import Row
from pyspark.sql.context import SQLContext
from Pycrypto import Spark_Session
from pyspark.sql.functions import *
from pyspark.sql.window import *
import urllib.request, json
import pandas as pd

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
    return json_string


def parse_json(json_data,sc):
    l = tuple()
    r = convert_to_line(json_data)
    mylist = []
    for line in r.splitlines():
        mylist.append(line)
        l =tuple(mylist)
    rdd = sc.parallelize(l,200)
    df = sqlContext.read.json(rdd)
    return df

if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext= SQLContext(conn)

#https://api.github.com/repos/pydata/pandas/issues?per_page=5'
with urllib.request.urlopen("http://carlofontanos.com/api/tutorials.php?data=all") as url:
    data1 = parse(url.read().decode("utf-8"))
    #print(data1)
    v= conn.parallelize(data1)

data = parse_json(data1,conn)
data.show()
print(data.count())

data2 =pd.read_json("https://api.github.com/repos/pydata/pandas/issues?per_page=5")
#print(data2[['created_at', 'title', 'body', 'comments']])