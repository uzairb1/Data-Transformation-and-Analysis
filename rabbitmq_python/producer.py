from time import sleep
from json import dumps
import json
import pika
import pandas as pd
import random

def producer():
    data = pd.read_csv("..//de_challenge_sample_data.csv")
    data['meta_dt'] = pd.to_datetime(data["meta_dt"]).dt.minute
    times = data.meta_dt
    de_df = data.loc[data['wiki'] == 'dewiki']
    d = {}
    for name, group in data.groupby(times):
        id = 'group_' + str(name)
        d[id] = group.to_json()
    

    credentials = pika.PlainCredentials("user","pass")
    connection = pika.BlockingConnection(
         pika.ConnectionParameters(host="localhost",credentials= credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange='globalen_anzahl', exchange_type='fanout')
    #print(type(d['group_12']))

    for item in d.items():
        #print(json.dumps(item))
        channel.basic_publish(exchange='globalen_anzahl', routing_key='', body=json.dumps(item))
        
        sleep(random.random())

    #print(lis[1])
    #for i, x in enumerate(lis): 
        #channel.basic_publish(exchange='', routing_key='globalen_anzahl', body=lis)
        #sleep(random.random())
        #channel.basic_publish("deutsch","group_end")
        #producer.flush()
    #producer.close()
producer()
