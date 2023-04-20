import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json


producer = KafkaProducer(bootstrap_servers=['3.83.124.127:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

producer.send('demo_testing2',value="How are you")

producer.flush()



df = pd.read_csv("/Users/prachichavan/Desktop/indexProcessed.csv")

while True:
    dict_stock = df.sample(1).to_dict(orient="records")
    producer.send('demo_testing2',value = dict_stock)
    
    producer.flush()


