from kafka import KafkaConsumer
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from datetime import datetime
from time import sleep
import sys
import re



def kafka_connect():
    zkQuorum, topic = sys.argv[1:]
    print(zkQuorum)
    print(topic)
    consumer = KafkaConsumer(topic)

    for message in consumer:
        t = message.value.decode("utf-8")
        #print(t)

        v = t.split(',')
        for i in v:
            print(v)



if __name__ == '__main__':
    kafka_connect()



