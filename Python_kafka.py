from kafka import KafkaConsumer
from kafka import TopicPartition
consumer = KafkaConsumer('my_favorite_topic')
for msg in consumer:
    print(msg)

consumer = KafkaConsumer(bootstrap_servers='localhost:1234')
consumer.assign([TopicPartition('foobar', 2)])
msg = next(consumer)


def triple_value(num):
    """Function to get thrice the value of a number."""
    return 3*num
print(triple_value(4).__doc__)


dic= (('a', 1),('f', 2), ('g', 3))
print(dict(dic))
