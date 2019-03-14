'''
Created on Feb 28, 2019

@author: barnwaldo
'''
import gevent
import atexit
from functools import reduce
from itertools import filterfalse, chain, dropwhile
from utils.message import KMessage
from utils.kstore import KStore, KWindow
from core.ktable import KTable
from core.kstreambuilder import KStreamCloner, KStreamGenMaker, KProducer
from core.globalktable import GlobalKTable



class KStream(object):
    '''
    if windowing is to be used, it must be defined before calling a function that uses a windowed stream
    set individual parameters such as
        kstream.window.windowtype = 'hopping'
    see kwindow for details
    '''
     
    def __init__(self, stream):
        self.runflag = True
        self.gthreads = []
        self.stream = stream    
        self.stores = {}
        self.windows = {}
        self.producer = KProducer.getInstance().producer
        atexit.register(self.cleanup)
        
        
    def cleanup(self):
        self.runflag = False
        for thread in self.gthreads:
            thread.join()
            thread.kill()   
           
           
    def branch(self, predicates):
        '''
        TERMINAL -- new streams from current stream (KStream --> Kstream[])
        -> Predicates are function statements that return True/False where True means that record is added to this branch
        -> Predicates are evaluated in order. A record is placed to one and only one output stream on the first match
        -> Last condition should be true to return all values that are not in prior branches
        '''
        nbranch = len(predicates)
        newitr = []
        for queue in  KStreamCloner(self.stream, nbranch).gqueues:
            newitr.append(KStreamGenMaker(queue).stream())
            
        for i in range(nbranch):
            newitr[i] = filter(predicates[i], newitr[i])        
            for j in range(i + 1, nbranch):
                newitr[j] = filterfalse(predicates[i], newitr[j])

        newkstreams = []
        for itr in newitr:
            newkstreams.append(KStream(itr))

        return newkstreams
     
     
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
        # valfunc = (lambda kmsg: KeyValuePair(kmsg.key, func(kmsg.val)))     
        valfunc = (lambda kmsg: KMessage(kmsg.key, func(kmsg.val)))   
        self.stream = map(valfunc, self.stream)
        # flattenvalues = (lambda kmsg: [KeyValuePair(kmsg.key, val) for val in kmsg.val])
        flattenvalues = (lambda kmsg: [KMessage(kmsg.key, val) for val in kmsg.val])
        self.stream = chain.from_iterable(map(flattenvalues, self.stream))
        return self
     
     
    def foreach(self, func):
        '''
        TERMINAL - just applies func to key value pairs
        '''
        g = gevent.spawn(self.fxnhelper, self.stream, self.runflag, func)
        self.gthreads.append(g)
 
     
    def groupByKey(self):
        '''
        return stream with only records that have a key -- input to KTable
        '''
        self.stream = filter(self.stream, (lambda kmsg : kmsg.key != None)) 
        return self
 
     
    def groupBy(self, func):
        '''
        map records to new records and then
        return stream with only records that have a key -- input to KTable
        '''
        self.stream = filter(map(func, self.stream), (lambda kmsg : kmsg.key != None)) 
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
        # valfunc = (lambda kmsg: KeyValuePair(kmsg.key, func(kmsg.val)))
        valfunc = (lambda kmsg: KMessage(kmsg.key, func(kmsg.val)))
        self.stream = map(valfunc, self.stream) 
        return self

         
    def peeker(self, func):
        '''
        func is lambda or method that operates on the stream
        '''
        peekfunc = (lambda kmsg: self.peekhelper(func, kmsg))
        self.stream = map(peekfunc, self.stream) 
        return self
  
     
    def printer(self):
        '''
        TERMINAL - just print key value pairs to console
        '''
        g = gevent.spawn(self.printerhelper, self.stream, self.runflag)
        self.gthreads.append(g)
        
     
    def to(self, topic):
        '''
        TERMINAL - send all records to producer topic
        '''
        g = gevent.spawn(self.helper, self.stream, topic, self.runflag)
        self.gthreads.append(g)
        
        
    def table(self, stream, kstore):
        '''
        func must be kmsg -> kmsg  where kmsg = KeyValuePair
        '''
        queue = KStreamCloner(stream, 1).gqueues[0]
        stream = KStreamGenMaker(queue, kstore=kstore).stream()
        return stream
    
    
    def aggregate(self, initializer, func, materialized='dummy', windict={}, changelog=None, terminal=False):
        '''
        func is method for the value in the key/value pair
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        
        # populate table with create stream           
        if windict:
            wname = windict['windowname']
            if wname  not in self.windows:
                self.windows[wname] = KWindow(wname, windict)
            self.stream = self.agghelper(self.stream, self.stores[materialized], initializer, func, window=self.windows[wname])
        else:
            self.stream = self.agghelper(self.stream, self.stores[materialized], initializer, func)   
        
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
        # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return a KTable with stream and table
            return KTable(self.stream, materialized, cleartable=False)
    
    
    def reduce(self, initializer, materialized='dummy', windict={}, changelog=None, terminal=False):
        '''
        func is method for the value in the key/value pair
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
            
        # populate table with create stream
        if windict:
            wname = windict['windowname']
            if wname  not in self.windows:
                self.windows[wname] = KWindow(wname, windict)
            self.stream = self.agghelper(self.stream, self.stores[materialized], initializer, window=self.windows[wname])
        else:
            self.stream = self.agghelper(self.stream, self.stores[materialized], initializer)
            
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return a KTable with stream and table
            return KTable(self.stream, materialized, cleartable=False)
    
    
    def count(self, materialized='dummy', windict={}, changelog=None, terminal=False):
        '''
        func is method for the value in the key/value pair
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
            
        # populate table with create stream
        if windict:
            wname = windict['windowname']
            if wname  not in self.windows:
                self.windows[wname] = KWindow(wname, windict)
            self.stream = self.counthelper(self.stream, self.stores[materialized], window=self.windows[wname])
        else:
            self.stream = self.counthelper(self.stream, self.stores[materialized])
            
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return a KTable with stream and table
            return KTable(self.stream, materialized, cleartable=False)
       
    
    def printerhelper(self, stream, runflag):
        for kmsg in stream:
            if(not runflag): break
            print("key: {}, value: {}".format(kmsg.key, kmsg.val))
            gevent.sleep(0)
        
    
    def fxnhelper(self, stream, runflag, func):
        for kmsg in stream:
            if(not runflag): break
            func(kmsg)
            gevent.sleep(0)    
    
            
    def peekhelper(self, func, kmsg):
        func(kmsg)
        return kmsg
    
    
    def agghelper(self, stream, kstore, initializer, func=None, kwindow=None):
        queue = KStreamCloner(stream, 1).gqueues[0]
        stream = KStreamGenMaker(queue).stream()
        for kmsg in stream:               
            if kmsg.key in kstore:
                if func is None:
                    # reduce
                    if kwindow is None:
                        kstore[kmsg.key] += kmsg.val
                    else:
                        kwindow[kmsg.key] = kmsg
                        kstore[kmsg.key] = reduce((lambda x, y: x.val + y.val), kwindow[kmsg.key])
                else:
                    # generalized aggregation
                    if kwindow is None:
                        kstore[kmsg.key] += func(kmsg.val)
                    else:
                        kwindow[kmsg.key] = kmsg
                        kstore[kmsg.key] = reduce((lambda x, y: func(x.val) + func(y.val)), kwindow[kmsg.key])
            else:
                kstore[kmsg.key] = initializer
            kmsg.val = kstore[kmsg.key]
            yield kmsg
            
            
    def counthelper(self, stream, kstore, kwindow=None):
        queue = KStreamCloner(stream, 1).gqueues[0]
        stream = KStreamGenMaker(queue).stream()
        for kmsg in stream:
            if kwindow is None:
                if kmsg.key in kstore:
                    kstore[kmsg.key] += 1
                else:
                    kstore[kmsg.key] = 1
            else:
                kwindow[kmsg.key] = kmsg
                kstore[kmsg.key] = len(kwindow[kmsg.key])
            
            kmsg.val = kstore[kmsg.key]
            yield kmsg
            
    
    def helper(self, stream, topic, runflag):
        for kmsg in stream:
            if(not runflag): break
            self.producer.poll(0)
            key = str(kmsg.key)
            val = str(kmsg.val)
            self.producer.produce(topic, key=key.encode('utf-8'), value=val.encode('utf-8'), on_delivery=self.delivery)
            gevent.sleep(0)

            
    def delivery(self, err, msg):
        '''
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). 
        '''
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            pass
            # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))        
    
    
    def join(self, kobject, windict={}, changelog=None, terminal=False):        
        '''
        Must define kwindow parameters in windict on kobject is kstream
        '''
        # join to stream (use kwindow since must be windowed)
        if kobject is type(KStream):
            # make window stream for right stream
            if not windict: return
            wname = windict['windowname']
            if wname  not in self.windows:
                self.windows[wname] = KWindow(wname, windict)
            g = gevent.spawn(self.makewindow, kobject.stream, self.runflag)
            self.gthreads.append(g)
            # perform inner join
            self.stream = self.joinhelper(self.stream, self.windows[wname])
        
        elif kobject is type(KTable) or kobject is type(GlobalKTable):
            tname = kobject.materialized
            self.stores[tname] = kobject.stores[tname]
            self.stream = self.joinhelper(self.stream, self.stores[tname])
        
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self  
        
        
    def leftjoin(self, kobject, windict={}, changelog=None, terminal=False):        
        '''
        Must define kwindow parameters in windict on kobject is kstream
        '''
        # join to stream (use kwindow since must be windowed)
        if kobject is type(KStream):
            # make window stream for right stream
            if not windict: return
            wname = windict['windowname']
            if wname  not in self.windows:
                self.windows[wname] = KWindow(wname, windict)
            g = gevent.spawn(self.makewindow, kobject.stream, self.runflag)
            self.gthreads.append(g)
            # perform inner join
            self.stream = self.leftjoinhelper(self.stream, self.windows[wname])
        
        elif kobject is type(KTable) or kobject is type(GlobalKTable):
            tname = kobject.materialized
            self.stores[tname] = kobject.stores[tname]
            self.stream = self.leftjoinhelper(self.stream, self.stores[tname])
        
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self
            
            
    def outerjoin(self, kobject, windict={}, changelog=None, terminal=False):        
        '''
        Must define kwindow parameters in windict on kobject is kstream
        '''
        # join to stream (use kwindow since must be windowed)
        if kobject is type(KStream):
            # make window stream for right stream
            if not windict: return
            wname = windict['windowname']
            if wname  not in self.windows:
                self.windows[wname] = KWindow(wname, windict)
            g = gevent.spawn(self.makewindow, kobject.stream, self.runflag)
            self.gthreads.append(g)
            # perform inner join
            self.stream = self.outerjoinhelper(self.stream, self.windows[wname])
        
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
                # empty generator if terminal and return None
        if terminal:
            dropwhile(True, self.stream)
            return None
        else:
            # return self with the stream
            return self
            
            
    
    def makewindow(self, stream, kwindow, runflag):
        for kmsg in stream:
            if(not runflag): break
            kwindow[kmsg.key] = kmsg
            gevent.sleep(0)
            
            
    def joinhelper(self, stream, kwindow):
        for kmsg in stream:
            if kmsg.key in kwindow:
                for wmsg in kwindow[kmsg.key]:
                    wmsg.val = {'left': kmsg.val, 'right': wmsg.val}
                    wmsg.offset = kmsg.offset
                    wmsg.partition = kmsg.partition
                    wmsg.time = kmsg.time
                    yield wmsg 
                          
                          
    def leftjoinhelper(self, stream, kwindow):
        for kmsg in stream:
            if kmsg.key in kwindow:
                for wmsg in kwindow[kmsg.key]:
                    wmsg.val = {'left': kmsg.val, 'right': wmsg.val}
                    wmsg.offset = kmsg.offset
                    wmsg.partition = kmsg.partition
                    wmsg.time = kmsg.time
                    yield wmsg 
            else:
                kmsg.val = {'left': kmsg.val, 'right': None}
                yield  kmsg            
                          
                          
    def outerjoinhelper(self, stream, kwindow):
        for kmsg in stream:
            if kmsg.key in kwindow:
                for key in kwindow:
                    for wmsg in kwindow[key]:
                        if kmsg.key == wmsg.key:
                            wmsg.val = {'left': kmsg.val, 'right': wmsg.val}
                            wmsg.offset = kmsg.offset
                            wmsg.partition = kmsg.partition
                            wmsg.time = kmsg.time
                        else:
                            wmsg.val = {'left': None, 'right': wmsg.val}                           
                        yield wmsg
                    
            else:
                for key in kwindow:
                    for wmsg in kwindow[key]:
                        wmsg.val = {'left': None, 'right': wmsg.val}                           
                        yield wmsg
            
                kmsg.val = {'left': kmsg.val, 'right': None}
                yield  kmsg 

 
 
                          
