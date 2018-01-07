from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, DataFrame, SQLContext
from pyspark.storagelevel import StorageLevel

def updateTotalCount(currentCount, countState):
    if countState is None:
       countState = 0
    return sum(currentCount, countState)


sc = SparkContext(appName="PythonStreamingKafkaWordCount")
ssc = StreamingContext(sc, 5)
sql = SQLContext(sc)

lines =ssc.socketTextStream("localhost",9999)

ssc.checkpoint("/Users/shuvamoymondal/chk1/*")

countStream = lines.flatMap(lambda line: line.split(" "))\
                   .map(lambda word: (word, 1))\
                   .reduceByKey(lambda a, b: a + b)

totalCounts = countStream.updateStateByKey(updateTotalCount)
totalCounts.pprint()
cntByKey  = lines.flatMap(lambda c: c.split(" ")).countByValue()
words = lines.map(lambda line: line.split(" ")).map(lambda x: (x,1))

#print(words.pprint())
#print("cntByKey",cntByKey.pprint())
ssc.start()
ssc.awaitTermination()