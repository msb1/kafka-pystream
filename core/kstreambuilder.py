'''
Created on Feb 22, 2019

@author: Barnwaldo
'''

import atexit
import gevent
import time
from random import randint
from gevent.queue import Queue
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from utils.message import serde, KMessage

     
class KStreamBuilder(object):
    '''
    example configs = {'bootstrap.servers': '192.168.5.3:9092', 'group.id': 'barnwaldo', 'session.timeout.ms': 6000}
    '''
    def __init__(self, topics, configs, keyserde, valserde, globaltable=False):
        # if globalktable, then change to a unique group.id so all partitions are read to one consumer
        if globaltable:
            configs['group.id'] += str(randint(0,9)) + str(randint(0,9)) + str(randint(0,9)) 
        self.consumer = Consumer(configs)
        self.producer = Producer(configs).producer
        self.keyserde = keyserde
        self.valserde = valserde
        self.runflag = True
        self.kqueue = Queue(maxsize=1000)
        self.kthread = gevent.spawn(self.kafkaconsumer, self.consumer, topics)
        self.gthread = gevent.spawn(self.stream)
        atexit.register(self.cleanup)


    def kafkaconsumer(self, consumer, topics):      
        consumer.subscribe(topics) 
        notfull = True
        msg = None
        while self.runflag:
            try:
                # if queue is not full, then pull new message
                # otherwise, try to put last message in queue
                if notfull:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        raise KafkaException(msg.error())              
                self.kqueue.put(msg, block=True, timeout=1.0)  
                # queue is not full since Full exception not raised
                notfull = True
                #print(msg.key().decode('utf-8'), msg.value().decode('utf-8'))
                gevent.sleep(0)
            except KafkaException:
                gevent.sleep(0.5)
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("KafkaException: ", msg.error())
                else:
                    continue
            except gevent.queue.Full:
                # queue Full exception raised 
                notfull = False
                gevent.sleep(0.5)

    def stream(self):
        while self.runflag:
            try:
                msg = self.kqueue.get(block=False)  
                # kmsg = KeyValuePair(serde(msg.key(), self.keyserde), serde(msg.value(), self.valserde))  
                kmsg = KMessage(serde(msg.key(), self.keyserde), serde(msg.value(), self.valserde))
                kmsg.partition = msg.partition()
                kmsg.offset = msg.offset()
                kmsg.time = time.time()     
                yield kmsg
                gevent.sleep(0)
            except gevent.queue.Empty:
                gevent.sleep(0.5)
                
       
    def cleanup(self):  
        self.runflag = False
        self.kthread.join()
        self.gthread.join()
        self.kthread.kill()
        self.gthread.kill()
        
        
class KStreamCloner(object):
    '''
    classdocs
    '''
    def __init__(self, gen, num):
        self.gen = gen
        self.num = num
        self.runflag = True
        self.gqueues = [Queue(maxsize=1000) for _ in range(num)]
        self.kthread = gevent.spawn(self.makequeues, gen)
        atexit.register(self.cleanup)

    def makequeues(self, gen):      
        notfull = True
        kmsg = None
        while self.runflag:
            try:
                # if queue is not full, then pull new message
                # otherwise, try to put last message in queue
                if notfull:
                    kmsg = next(gen)
                    if kmsg is None:
                        continue 
                for q in self.gqueues: 
                    if q.full(): raise gevent.queue.Full
                for q in self.gqueues:            
                    q.put(kmsg)  
                # queue is not full since Full exception not raised
                notfull = True
                #print(msg.key().decode('utf-8'), msg.value().decode('utf-8'))
                gevent.sleep(0)
            except gevent.queue.Full:
                # queue Full exception raised 
                notfull = False
                gevent.sleep(0.5)
                
       
    def cleanup(self):  
        self.runflag = False
        self.kthread.join()
        self.kthread.kill()
        

class KStreamGenMaker(object):
    '''
    classdocs
    '''
    def __init__(self, gqueue, kstore=None):
        self.gqueue = gqueue 
        self.runflag = True
        self.kstore = kstore
        self.gthread = gevent.spawn(self.stream)
        atexit.register(self.cleanup)


    def stream(self):
        while self.runflag:
            try:
                kmsg = self.gqueue.get(block=False)  
                # if kstore is None, then stream not table
                if self.kstore is not None:            
                    self.kstore[kmsg.key] = kmsg.val          
                yield kmsg
                gevent.sleep(0)
            except gevent.queue.Empty:
                gevent.sleep(0.5)
     
          
    def cleanup(self):  
        self.runflag = False
        self.gthread.join()
        self.gthread.kill()


class KProducer(object):
    '''
    configs = {'bootstrap.servers': '192.168.5.3:9092', 'session.timeout.ms': 6000}
    '''
    
    __instance = None
    @staticmethod 
    def getInstance():
        """ Static access method. """
        if KProducer.__instance == None:
            KProducer()
        return KProducer.__instance
 
   
    def __init__(self, configs):
        """ Virtually private constructor. """
        if KProducer.__instance == None:
            self.producer = Producer(configs)
            KProducer.__instance = self


    
    
    
        