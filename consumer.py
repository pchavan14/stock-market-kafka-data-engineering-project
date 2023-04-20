from kafka import KafkaConsumer
from time import sleep
from json import loads
import json
from s3fs import S3FileSystem

consumer = KafkaConsumer(
    'demo_testing2',
     bootstrap_servers=['3.83.124.127:9092'], #add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# for c in consumer:
#     print(c)

s3 = S3FileSystem()

for count , i in enumerate(consumer):
    with s3.open("s3://kafka-stock-market/stock_market_{}.csv".format(count),'w') as file:
        json.dump(i.value,file)









