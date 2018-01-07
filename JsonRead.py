from pyspark.sql.context import SQLContext
from pyspark.sql.functions import lit,col
from Pycrypto import Spark_Session
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *
from pyspark.sql.window import *
import urllib.request, json
import requests
if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sqlContext = SQLContext(conn)


##https://api.github.com/users?since=100
#with urllib.request.urlopen("https://api.github.com/users?since=100") as url:
 #   data = url.read().decode("utf-8")
  #  v =json.load(data)
   # print(type(v))
   # data = {'people': [{'name': 'Scott', 'website': 'stackabuse.com', 'from': 'Nebraska'}]}
b = requests.get(url = "http://carlofontanos.com/api/tutorials.php?data=all")
n = b.json()
m=tuple(n)
v = conn.parallelize(m,80)
j =v.map(lambda h: h.split(":")).map(lambda k: k[0])
print(j.collect())

#df = sqlContext.createDataFrame([json.loads(line) for line in b.splitlines()])
#df.show()