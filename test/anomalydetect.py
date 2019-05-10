'''
Created on Mar 26, 2019

@author: barnwaldo
'''

import time
from core.kbuilder import KStreamBuilder, KStream
from core.kutils import KeyValuePair, KWindow
from kafka import KafkaProducer
from random import randint
from multiprocessing import Process

# config info for Kafka broker
config = {'bootstrap.servers': '192.168.21.3:9092', 'group.id': 'barnwaldo', 'session.timeout.ms': 6000}
names = ['bob', 'alice', 'ted', 'samantha', 'vyra', 'frosty', 'abe']
topics = 'clicks'

def peek(kv):
    print("key: {} -- value: {}".format(kv.key, kv.val))
    return kv


def make_clicks():
    producer = KafkaProducer(bootstrap_servers=config['bootstrap.servers'])
    print('make_clicks')
    while True:
        name = names[randint(0, 6)]
        producer.send(topics, key='0'.encode('utf-8'), value=name.encode('utf-8')) 
        # print(name,'sent...')
        # sleep for random interval
        interval = randint(2, 15) * 0.5
        # print('interval', interval)
        time.sleep(interval)


if __name__ == '__main__':
    p = Process(target=make_clicks)
    p.start()
    # define builder for streams/table
    builder = KStreamBuilder(topics, config, keyserde='str', valserde='str')
    # initialize Kstream with builder
    views = KStream(builder.builder)

    views.map(lambda kv: KeyValuePair(kv.val, kv.val, time=kv.time))
    
    anamolous_users = views.count(materialized='usercounts', kwindow=KWindow(60)).filter(lambda kv: kv.val >= 3)

    # change table back to stream and then send output to Kafka topic
    anamolous_users.toStream().peeker(peek).to('anomalous', config, keyserde='str', valserde='str')

    # Note that KStream can either be initialized with a KStreambuilder or a stream (generator) with a Kafka Producer
    
    