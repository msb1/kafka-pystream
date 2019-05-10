'''
Created on Feb 22, 2019

@author: Barnwaldo
'''
import time
import atexit
from kafka import KafkaConsumer
from multiprocessing import Process, Queue
from functools import reduce
from itertools import filterfalse, chain, dropwhile

from core.kutils import KeyValuePair, KStore, serde, kqueue_maker, kproducer

#################################################################################################################
#################################################################################################################
#################################################################################################################
     
class KStreamBuilder(object):
    '''Class to create initial builder for kstreams, ktables and global ktables
    
        --initiates a process for a kafka consumer for topic
        --consumer generates messages for multiprocess queue --> builder
        
        usage: KStream(builder) or KTable(builder)
    
        example configs = {'bootstrap.servers': '192.168.21.3:9092', 'group.id': 'barnwaldo', 'session.timeout.ms': 6000}
    '''
    def __init__(self, topic, configs, keyserde='str', valserde='str', globaltable=False):      
        # if globalktable, then change to a unique group.id so all partitions are read to one consumer
        if globaltable:
            configs['group.id'] += str(id(self))
        self.consumer = KafkaConsumer(bootstrap_servers=configs['bootstrap.servers'], 
                                      group_id=configs['group.id'],
                                      session_timeout_ms=configs['session.timeout.ms'])
        self.connected = False
        self.keyserde = keyserde          
        self.valserde = valserde
        self.topic = topic
        self.builder = {}
        self.builder = Queue()
        # start consumer Process
        self.kthread = Process(target=self.kafkaconsumer)
        self.kthread.start()
        atexit.register(self.cleanup)
        print("KStreamBuilder init complete... topic:", self.topic)      
        

    def cleanup(self):
        self.kthread.join()
        
    def kafkaconsumer(self):   
        print("Kafka Consumer START... topic:", self.topic)
        self.consumer.subscribe(self.topic)
        for msg in self.consumer:
            # print original message
            # print ("%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value))
            # convert message to KeyValuePair
            kv = KeyValuePair(serde(msg.key, self.keyserde), serde(msg.value, self.valserde))
            kv.partition = msg.partition
            kv.offset = msg.offset
            kv.time = time.time()   
            # write message to queue for topic
            self.builder.put_nowait(kv)
            
#################################################################################################################
#################################################################################################################
#################################################################################################################

class KStream(object):
    '''Class to create a KStream
    
        --KStream (and KTable) must be initialized with a KStreamBuilder builder queue
        --if windowing is to be used, it must be defined before calling a function that uses a windowed stream
           set individual parameters such as kstream.window.windowtype = 'hopping' (see kwindow for details)
        --close analog to Java KafkaStreams 
            >> all stream to stream functions, stream to table functions and joins have same outputs
    '''
     
    def __init__(self, queue):
        self.builder = None      # for queue if needed for KTable or something else
        self.bqueues = []        # list for branched queues
        self.pthreads = []
        self.stream = iter(queue.get, 'xxx')     
        atexit.register(self.cleanup)
        print("KStream init complete...")
        

    def cleanup(self):
        for thread in self.pthreads:
            thread.join()

                          
    def branch(self, predicates):
        '''
        TERMINAL -- new builder queues from current stream (KStream --> builder[])
        -> Predicates are function statements that return True/False where True means that record is added to this branch
        -> Predicates are evaluated in order. A record is placed to one and only one output stream on the first match
        -> Last condition should be true to return all values that are not in prior branches
        '''
        nbranch = len(predicates)
        newkstreams = []        
        self.bqueues.clear()    
        
        for i in range(nbranch):
            self.bqueues.append(Queue())
            newkstreams.append(KStream(self.bqueues[i]))  
        
        p = Process(target=self.branch_helper, args=(nbranch, predicates,))
        p.start()
        self.pthreads.append(p)                                          
        return newkstreams
    
    
    def branch_helper(self, nbranch, predicates):
        
        for kv in self.stream:
            for i in range(nbranch):
                if predicates[i](kv):
                    self.bqueues[i].put(kv)
                    break
        
    
    def filter(self, func):
        '''
        func is lambda or method that returns true/false where true keeps record and false discards record
        '''
        self.stream = filter(func, self.stream)  
        return self
     
     
    def filterNot(self, func):
        '''
        func is lambda or method that returns true/false where true discards record and false keeps record
        '''
        self.stream = filterfalse(func, self.stream)  
        return self
     
     
    def flatMap(self, func):
        '''
        func is lambda or method that changes a key/value pair into a list of new key/value pairs to be flattened   
        '''
        self.stream = chain.from_iterable(map(func, self.stream))
        return self
     
     
    def flatMapValues(self, func):
        '''
        func is lambda or method that changes values to list of values to be flattened
        ''' 
        # print('flatmapvalues')
        valfunc = (lambda kv: KeyValuePair(kv.key, func(kv.val)))   
        self.stream = map(valfunc, self.stream)
        flattenvalues = (lambda kv: [KeyValuePair(kv.key, val) for val in kv.val])
        self.stream = chain.from_iterable(map(flattenvalues, self.stream))
        return self
     
     
    def foreach(self, func):
        '''
        TERMINAL - just applies func to key value pairs
        '''
        p = Process(target=self.for_helper, args=(self.stream, func,))
        p.start()
        self.pthreads.append(p)
         
         
    def for_helper(self, stream, func):
        for kv in stream:
            func(kv)
  
     
    def groupByKey(self):
        '''
        return stream with only records that have a key -- input to KTable
        '''
        self.stream = filter(self.stream, (lambda kv : kv.key != None)) 
        return self
 
     
    def groupBy(self, func):
        '''
        map records to new records and then
        return stream with only records that have a key -- input to KTable
        '''
        # print('groupby')
        self.stream = filter(lambda kv : kv.key != None, map(func, self.stream)) 
        return self
 
     
    def map(self, func):
        '''
        func is lambda or method that changes keys and values 
        '''
        self.stream = map(func, self.stream)  
        return self
 
     
    def mapValues(self, func):
        '''
        func is lambda or method that changes values 
        '''
        valfunc = (lambda kv: KeyValuePair(kv.key, func(kv.val)))
        self.stream = map(valfunc, self.stream) 
        return self

         
    def peeker(self, func):
        '''
        func is lambda or method that operates on the stream
        '''
        #peekfunc = (lambda kv: self.peek_helper(func, kv))
        self.stream = map(func, self.stream) 
        return self
 
             
    def peek_helper(self, func, kv):
        func(kv)
        return kv
  
     
    def printer(self):
        '''
        TERMINAL - just print key value pairs to console
        '''
        # print('printer')
        p = Process(target=self.printer_helper, args=(self.stream,))
        p.start()
        self.pthreads.append(p)
        
    
    def printer_helper(self, stream):
        # print('printerhelper')
        for kv in stream:
            print("key: {}, value: {}".format(kv.key, kv.val))
        
      
    def to(self, topic, configs, keyserde='str', valserde='str'):
        '''
        TERMINAL - send all records to producer topic
        '''
        # print('kstream --to--')
        p = Process(target=kproducer, args=(self.stream, topic, configs['bootstrap.servers'], keyserde, valserde,))
        p.start()
        self.pthreads.append(p)
     
       
    def aggregate(self, initializer, func, materialized='agg', cleartable=True, kwindow=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):
        '''
        func is method for the value in the key/value pair
        '''
        # create a KTable with no init
        table_queue = Queue()
        p = Process(target=kqueue_maker, args=(self.stream, table_queue,))
        p.start()
        self.pthreads.append(p)
        ktable = KTable(table_queue, materialized=materialized, cleartable=cleartable, init=False)
         
        # populate table with create stream           
        if kwindow is None:         
            ktable.stream = self.agghelper(ktable.stream, ktable.kstores[materialized], initializer, func, window=False)
        else:
            ktable.stream = self.agghelper(kwindow.window(ktable.stream), ktable.kstores[materialized], initializer, func, window=True)
         
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(ktable.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
        
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, ktable.stream)
            return None

        return ktable
     
     
    def reduce(self, initializer, materialized='red', cleartable=True, kwindow=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):
        '''
        '''
        # create a KTable with no init
        table_queue = Queue()
        p = Process(target=kqueue_maker, args=(self.stream, table_queue,))
        p.start()
        self.pthreads.append(p)
        ktable = KTable(table_queue, materialized=materialized, cleartable=cleartable, init=False)
             
        # populate table with create stream           
        if kwindow is None:         
            ktable.stream = self.agghelper(ktable.stream, ktable.kstores[materialized], initializer, window=False)
        else:
            ktable.stream = self.agghelper(kwindow.window(ktable.stream), ktable.kstores[materialized], initializer, window=True)
             
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(ktable.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
        
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, ktable.stream)
            return None

        return ktable
 

    def agghelper(self, stream, kstore, initializer, func=None, window=False):
        for kv in stream:               
            if kv.key in kstore:
                if func is None:
                    # reduce
                    if window:
                        kstore[kv.key] = reduce((lambda x, y: x.val + y.val), kv.val)
                    else:
                        kstore[kv.key] += kv.val
                else:
                    # generalized aggregation
                    if window:
                        kstore[kv.key] = reduce((lambda x, y: func(x.val) + func(y.val)), kv.val)
                    else:
                        kstore[kv.key] += func(kv.val)
            else:
                kstore[kv.key] = initializer
            kv.val = kstore[kv.key]
            yield kv
    
     
    def count(self, materialized='cnt', cleartable=True, kwindow=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):
        '''
        '''
        # create a KTable with no init
        table_queue = Queue()
        p = Process(target=kqueue_maker, args=(self.stream, table_queue,))
        p.start()
        self.pthreads.append(p)
        ktable = KTable(table_queue, materialized=materialized, cleartable=cleartable, init=False)
             
        # populate table with create stream           
        if kwindow is None:         
            ktable.stream = self.counthelper(ktable.stream, ktable.kstores[materialized], window=False)
        else:
            ktable.stream = self.counthelper(kwindow.window(ktable.stream), ktable.kstores[materialized], window=True)               
             
        # print('kstream --count--')
        
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(ktable.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, ktable.stream)
            return None
        
        return ktable
                   
             
    def counthelper(self, stream, kstore, window=False):
        for kv in stream:
            if window:
                ctr = 0
                # print(kv.key, kv.val, len(kv))
                for item in kv.val:
                    if(item.key == kv.key): 
                        ctr += 1
                kstore[kv.key] = ctr
            else:
                if kv.key in kstore:
                    kstore[kv.key] += 1
                else:
                    kstore[kv.key] = 1           
            kv.val = kstore[kv.key]
            yield kv            
     
     
    def join(self, kobject, value_joiner=None, key_mapper=None, kwindow=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):        
        '''
        '''
        # print("--join-start--", type(kobject))
        if kobject is type(KStream):
            # join two windowed streams
            if kwindow is None: return self
            # perform inner join
            # print("--join-stream--")
            self.stream = self.joinhelper(kwindow.window(self.stream), kwindow.window(kobject.stream), value_joiner=value_joiner, table=False)
         
        else:
            # join stream to table
            right = None
            if isinstance(kobject, KTable):
                right = kobject.stores[kobject.materialized]
            elif isinstance(kobject, KGlobalTable):
                right = kobject.store
            else:
                print("type is NONE")
                return self
            # print("--join-table--")
            self.stream = self.joinhelper(self.stream, right, key_mapper=key_mapper, value_joiner=value_joiner, table=True)
         
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self  
         
         
    def leftjoin(self, kobject, value_joiner=None, key_mapper= None, kwindow=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):        
        '''
        '''
        if kobject is type(KStream):
            # join two windowed streams
            if kwindow is None: return
            # perform left join
            self.stream = self.leftjoinhelper(kwindow.window(self.stream), kwindow.window(kobject.stream), value_joiner, toTable=False)
         
        elif kobject is type(KTable):
            # left join stream to table
            tname = kobject.materialized
            self.stores[tname] = kobject.stores[tname]
            self.stream = self.leftjoinhelper(self.stream, self.stores[tname], value_joiner, toTable=True)
             
        elif kobject is type(KGlobalTable):
            self.stream = self.leftjoinhelper(self.stream, kobject.store, value_joiner, key_mapper=key_mapper, toTable=True)
         
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self
             
             
    def outerjoin(self, kobject, value_joiner=None, kwindow=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):        
        '''
        '''
        if kobject is type(KStream):
            # join two windowed streams
            if kwindow is None: return
            # perform left join
            self.stream = self.outerjoinhelper(kwindow.window(self.stream), kwindow.window(kobject.stream), value_joiner)
         
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self
                        
             
    def joinhelper(self, left, right, key_mapper=None, value_joiner=None, table=False):
        # print("JOIN HELPER")
        if value_joiner is None:
            for kv in left:
                # print("none", kv)
                yield kv
              
        if table:
            # if stream/table join - left object is stream and right object is kstore (table)
            for kv in left:
                if key_mapper is not None:
                    # print("unmapped-key: ", kv.key, "unmapped-value: ", kv.val, isinstance(kv.val,(dict,)))
                    kv.key = key_mapper(kv)
                    # print("mapped-key: ", kv.key)
                if kv.key in right:
                    kv.val = value_joiner(kv.val, right[kv.key])
                    # print("key:", kv.key, "  joiner: ",kv.val, kv.val.__dict__)
                    yield kv    
                        
        else:
            # if windowed stream/stream join - have two streams of windowed lists as leftobject, rightobject
            left_list = next(left)
            right_list = next(right)
             
            for kvl in left_list:
                for kvr in right_list:
                    # print("stream", kvl, kvr)
                    if kvl.key == kvr.key:
                        kvr.val = value_joiner(kvl.val, kvr.val)
                        kvr.offset = kvl.offset
                        kvr.partition = kvl.partition
                        kvr.time = kvl.time
                        yield kvr
                           
                           
    def leftjoinhelper(self, leftobject, rightobject, value_joiner, key_mapper=None, toTable=False, globaltable=False):
         
        if toTable:
            # if stream/table join - left object is stream and right object is kstore (table)
            for kv in leftobject:
                if globaltable and key_mapper is not None:
                    kv.key = key_mapper(kv.key, kv.val)
                if kv.key in rightobject:
                    kv.val = value_joiner(kv.val, rightobject[kv.key])   
                # yield value same if not in rightobject
                yield kv
        else:
            # if windowed stream/stream join - have two streams of windowed lists as leftobject, rightobject
            for left, right in leftobject, rightobject:
                for kvl in left:
                    left_key_not_in_right = False
                    for kvr in right:
                        if kvl.key == kvr.key:
                            kvr.val = value_joiner(kvl.val, kvr.val)
                        elif left_key_not_in_right: 
                            continue
                        else:
                            kvr.val = kvl.val
                            left_key_not_in_right = True
                        kvr.key = kvl.key
                        kvr.offset = kvl.offset
                        kvr.partition = kvl.partition
                        kvr.time = kvl.time
                        yield kvr        
                           
                           
    def outerjoinhelper(self, leftobject, rightobject, value_joiner):
        # windowed stream/stream join - have two streams of windowed lists as leftobject, rightobject
        for left, right in leftobject, rightobject:
            # inner join and left join (not in inner join)
            for kvl in left:
                left_key_not_in_right = False
                for kvr in right:
                    if kvl.key == kvr.key:
                        kvr.val = value_joiner(kvl.val, kvr.val)
                    elif left_key_not_in_right: 
                        continue
                    else:
                        kvr.val = kvl.val
                        left_key_not_in_right = True
                    kvr.key = kvl.key
                    kvr.offset = kvl.offset
                    kvr.partition = kvl.partition
                    kvr.time = kvl.time
                    yield kvr 
             
            # right join keys not in left join
            for kvr in right:
                for kvl in left:
                    if kvr.key != kvl.key:
                        kvl.val = kvr.val
                        kvl.key = kvr.key
                        kvl.offset = kvr.offset
                        kvl.partition = kvr.partition
                        kvl.time = kvr.time
                        yield kvl
                        break

#################################################################################################################
#################################################################################################################
#################################################################################################################


class KTable(object):
    '''Class to create a KTable
    
        --KTable (and KStream must be initialized with a KStreamBuilder builder queue
        --close analog to Java KafkaStreams 
            >> all table to table functions, table to stream functions and joins have same outputs
    '''
     
    def __init__(self, queue, materialized='dummy', cleartable=True, init=True):
        self.pthreads = []
        self.kstores = {}
        self.builder = None
        self.materialized = materialized
        self.kstores[materialized] = KStore(materialized, cleartable=cleartable)   
        self.stream = iter(queue.get, 'xxx')    
        if init: 
            self.stream = self.table(self.stream, self.kstores[materialized])
        atexit.register(self.cleanup)
        print("KTable init complete...")
        
        
    def cleanup(self):
        for thread in self.pthreads:
            thread.join()
           
           
    def table(self, stream, kstore):
        for kv in stream:
            kstore[kv.key] = kv.val
            yield kv
     
     
    def filter(self, func):
        '''
        func is lambda or method that returns true/false where true keeps record and false discards record
        '''
        materialized = self.materialized + '_filter'
        self.kstores[materialized] = KStore(materialized)
        # populate table
        self.stream = self.table(filter(func, self.stream) , self.kstores[materialized])  
        self.materialized = materialized
        return self
     
     
    def filterNot(self, func):
        '''
        func is lambda or method that returns true/false where true keeps record and false discards record
        '''
        materialized = self.materialized + '_filternot'
        self.kstores[materialized] = KStore(materialized)
        # populate table
        self.stream = self.table(filterfalse(func, self.stream) , self.kstores[materialized])  
        self.materialized = materialized
        return self
        
    
    def mapValues(self, func):
        '''
        func is lambda or method that changes values 
        '''
        materialized = self.materialized + '_mapped'
        self.kstores[materialized] = KStore(materialized)
        # populate table
        valfunc = (lambda kv: KeyValuePair(kv.key, func(kv.val)))   
        self.stream = self.table(map(valfunc, self.stream)  , self.kstores[materialized]) 
        self.materialized = materialized
        return self   
    
    
    def aggregate(self, initializer, func, materialized='dummy', changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):
        '''
        func is method for the value in the key/value pair
        '''
        # if store does not exist, create it
        if materialized not in self.kstores:
            self.kstores[materialized] = KStore(materialized)
        # populate table with create stream
        self.stream = self.agghelper(self.stream, self.kstores[materialized], self.kstores[self.table], initializer, func)
        self.materialized = materialized
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            return self
    
    
    def reduce(self, initializer, materialized='dummy', changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):
        '''
        func is method for the value in the key/value pair
        '''
        # if store does not exist, create it
        if materialized not in self.kstores:
            self.kstores[materialized] = KStore(materialized)
        # populate table with create stream
        self.stream = self.agghelper(self.stream, self.kstores[materialized], self.kstores[self.table], initializer)
        self.materialized = materialized        
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return a KTable with stream and table
            return self


    def agghelper(self, stream, newkstore, oldkstore, initializer, func=None):
        for kv in stream:
            if kv.key in newkstore:
                oldval = 0
                if kv.key in oldkstore:
                    oldval = oldkstore[kv.key]            
                if func is None:
                    # reduce
                    newkstore[kv.key] += kv.val - oldval
                else:
                    # generalized aggregation
                    newkstore[kv.key] += func(kv.val) - func(oldval)
            else:
                newkstore[kv.key] = initializer
            kv.val = newkstore[kv.key]
            yield kv
 

    def toStream(self):
        stream_queue = Queue()
        p = Process(target=kqueue_maker, args=(self.stream, stream_queue,))
        p.start()
        self.pthreads.append(p)
        return KStream(stream_queue)
            
            
    def join(self, ktable, materialized='dummy', value_joiner=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):        
        '''
        '''
        # if store does not exist, create it
        if materialized not in self.kstores:
            self.kstores[materialized] = KStore(materialized)
        
        # join to stream (use kwindow since must be windowed)
        if ktable is not type(KTable): return

        self.kstores[ktable.materialized] = ktable.kstores[ktable.materialized]
        self.stream = self.joinhelper(self.stream,
                                      self.kstores[self.materialized],       # left KTable
                                      self.kstores[ktable.materialized],     # right KTable
                                      self.kstores[materialized],            # joined KTable
                                      value_joiner)            
        self.materialized = materialized
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self  
              
            
    def joinhelper(self, stream, left, right, new, value_joiner):
        for kv in stream:
            for key in left:
                if key in right:
                    new[key] = value_joiner(left[key], right[key])
            yield kv      
        
        
    def leftjoin(self, ktable, materialized='dummy', value_joiner=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):        
        '''
        '''
        # if store does not exist, create it
        if materialized not in self.kstores:
            self.kstores[materialized] = KStore(materialized)
        
        # join to stream (use kwindow since must be windowed)
        if ktable is not type(KTable): return

        self.kstores[ktable.materialized] = ktable.kstores[ktable.materialized]
        self.stream = self.leftjoinhelper(self.stream,
                                          self.kstores[self.materialized],       # left KTable
                                          self.kstores[ktable.materialized],     # right KTable
                                          self.kstores[materialized],            # joined KTable
                                          value_joiner)
        self.materialized = materialized
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self
    
    
    def leftjoinhelper(self, stream, left, right, new, value_joiner):
        for kv in stream:
            for key in left:
                if key in right:
                    new[key] = value_joiner(left[key], right[key])
                else:
                    new[key] = left[key]
            yield kv          
            
            
    def outerjoin(self, ktable, materialized='dummy', value_joiner=None, changelog=None, terminal=False, configs={'bootstrap.servers':'localhost:9092'}):        
        '''
        '''
        # if store does not exist, create it
        if materialized not in self.kstores:
            self.kstores[materialized] = KStore(materialized)
        
        # join to stream (use kwindow since must be windowed)
        if ktable is not type(KTable): return

        self.kstores[ktable.materialized] = ktable.kstores[ktable.materialized]
        self.stream = self.outerjoinhelper(self.stream,
                                           self.kstores[self.materialized],       # left KTable
                                           self.kstores[ktable.materialized],     # right KTable
                                           self.kstores[materialized],            # joined KTable
                                           value_joiner)            
        self.materialized = materialized
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            p = Process(target=kproducer, args=(self.stream, changelog, configs['bootstrap.servers'], 'str', 'str',))
            p.start()
            self.pthreads.append(p)
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self
                      
                          
    def outerjoinhelper(self, stream, left, right, new, value_joiner):
        for kv in stream:
            for lkey in left:
                if lkey in right:
                    for rkey in right:
                        if lkey == rkey:
                            new[rkey] = value_joiner(left[lkey], right[rkey])
                        elif rkey not in new or new[rkey] != right[rkey]:
                            new[rkey] = right[rkey]   
                else:                    
                    new[lkey] = left[lkey]
            yield kv         
                        
            
        
    



#################################################################################################################
#################################################################################################################
#################################################################################################################

class KGlobalTable(object):
    '''Class to create a Global KTable
    
        --KGlobalTable must be initialized with a KStreamBuilder builder queue
        --KStreamBuilder must have globaltable parameter set to true so that all partitions from a topic are read 
          into Global KTable
        --close analog to Java KafkaStreams 
            >> can only be used with KStream joins
    '''
    def __init__(self, queue, materialized='dummy', cleartable=True):
        self.store = KStore(materialized, cleartable=cleartable)              
        self.stream = iter(queue.get, 'xxx') 
        self.pthread = Process(target=self.table)
        self.pthread.start()
        atexit.register(self.cleanup) 
        print("KGlobalTable init complete... materialized:", materialized)
        
        
    def cleanup(self):
        self.pthread.join()
           
           
    def table(self):
        '''
        TERMINAL since global table
        '''
        for kv in self.stream:
            # print("Global:", kv.key, kv.val)
            self.store[kv.key] = kv.val



#################################################################################################################
#################################################################################################################
#################################################################################################################                           

