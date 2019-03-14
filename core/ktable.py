'''
Created on Feb 28, 2019

@author: barnwaldo
'''
import atexit
from itertools import filterfalse, dropwhile
from core.kstreambuilder import KStreamCloner, KStreamGenMaker, KProducer
from utils.kstore import KStore

class KTable(object):
    '''
    classdocs
    '''
     
    def __init__(self, stream, materialized='dummy', cleartable=True):
        self.runflag = True
        self.gthreads = []
        self.stores = {}
        self.materialized = materialized
        self.stores[materialized] = KStore(materialized, cleartable=cleartable)              
        self.stream = self.table(stream, self.stores[materialized])
        self.producer = KProducer.getInstance().producer
        atexit.register(self.cleanup)
        
        
    def cleanup(self):
        self.runflag = False
        for thread in self.gthreads:
            thread.join()
            thread.kill()   
           
           
    def table(self, stream, kstore):
        queue = KStreamCloner(stream, 1).gqueues[0]
        stream = KStreamGenMaker(queue, kstore=kstore).stream()
        return stream
     
     
    def filter(self, func, materialized='dummy', changelog=None, terminal=False):
        '''
        func is lambda or method that returns true/false where true keeps record and false discards record
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        # populate table with create stream
        self.stream = self.table(filter(func, self.stream) , self.stores[materialized])  
        self.materialized = materialized
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
        # empty generator if terminal
        if terminal:
            dropwhile(True, self.stream)
        return self
     
     
    def filterNot(self, func, materialized='dummy', changelog=None, terminal=False):
        '''
        func is lambda or method that returns true/false where true keeps record and false discards record
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        # populate table with create stream
        self.stream = self.table(filterfalse(func, self.stream) , self.stores[materialized])  
        self.materialized = materialized
        # write new stream to changelog (which is topic name)
        if changelog is not None:
            self.stream = self.produce(self.stream, changelog, self.runflag)
        # empty generator if terminal
        if terminal:
            dropwhile(True, self.stream)
        return self
    
    
    def aggregate(self, initializer, func, materialized='dummy', changelog=None, terminal=False):
        '''
        func is method for the value in the key/value pair
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        # populate table with create stream
        self.stream = self.agghelper(self.stream, self.stores[materialized], self.stores[self.table], initializer, func)
        self.materialized = materialized
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
    
    
    def reduce(self, initializer, materialized='dummy', changelog=None, terminal=False):
        '''
        func is method for the value in the key/value pair
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        # populate table with create stream
        self.stream = self.agghelper(self.stream, self.stores[materialized], self.stores[self.table], initializer)
        self.materialized = materialized        
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


    def agghelper(self, stream, newkstore, oldkstore, initializer, func=None):
        queue = KStreamCloner(stream, 1).gqueues[0]
        stream = KStreamGenMaker(queue).stream()
        for kmsg in stream:
            if kmsg.key in newkstore:
                oldval = 0
                if kmsg.key in oldkstore:
                    oldval = oldkstore[kmsg.key]            
                if func is None:
                    # reduce
                    newkstore[kmsg.key] += kmsg.val - oldval
                else:
                    # generalized aggregation
                    newkstore[kmsg.key] += func(kmsg.val) - func(oldval)
            else:
                newkstore[kmsg.key] = initializer
            kmsg.val = newkstore[kmsg.key]
            yield kmsg
 

    def to(self):
        for kmsg in self.stream:
            kstore = self.kstore[self.materialized] 
            kmsg.val = kstore[kmsg.key]
            yield kmsg
            
            
    def join(self, ktable, materialized='dummy', changelog=None, terminal=False):        
        '''
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        
        # join to stream (use kwindow since must be windowed)
        if ktable is not type(KTable): return

        self.stores[ktable.materialized] = ktable.stores[ktable.materialized]
        self.stream = self.joinhelper(self.stream,
                                      self.stores[self.materialized],       # left KTable
                                      self.stores[ktable.materialized],     # right KTable
                                      self.stores[materialized])            # joined KTable
        self.materialized = materialized
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
        
        
    def leftjoin(self, ktable, materialized='dummy', changelog=None, terminal=False):        
        '''
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        
        # join to stream (use kwindow since must be windowed)
        if ktable is not type(KTable): return

        self.stores[ktable.materialized] = ktable.stores[ktable.materialized]
        self.stream = self.leftjoinhelper(self.stream,
                                          self.stores[self.materialized],       # left KTable
                                          self.stores[ktable.materialized],     # right KTable
                                          self.stores[materialized])            # joined KTable
        self.materialized = materialized
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
            
            
    def outerjoin(self, ktable, materialized='dummy', changelog=None, terminal=False):        
        '''
        '''
        # if store does not exist, create it
        if materialized not in self.stores:
            self.stores[materialized] = KStore(materialized)
        
        # join to stream (use kwindow since must be windowed)
        if ktable is not type(KTable): return

        self.stores[ktable.materialized] = ktable.stores[ktable.materialized]
        self.stream = self.outerjoinhelper(self.stream,
                                           self.stores[self.materialized],       # left KTable
                                           self.stores[ktable.materialized],     # right KTable
                                           self.stores[materialized])            # joined KTable
        self.materialized = materialized
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
            
            
    def joinhelper(self, stream, left, right, new):
        for kmsg in stream:
            for key in left:
                if key in right:
                    new[key] = {'left': left[key], 'right': right[key]} 
            yield kmsg
                          
                          
    def leftjoinhelper(self, stream, left, right, new):
        for kmsg in stream:
            for key in left:
                if key in right:
                    new[key] = {'left': left[key], 'right': right[key]} 
                else:
                    new[key] = {'left': left[key], 'right': None}
            yield kmsg           
                          
                          
    def outerjoinhelper(self, stream, left, right, new):
        for kmsg in stream:
            for lkey in left:
                if lkey in right:
                    for rkey in right:
                        if lkey == rkey:
                            new[rkey] = {'left': left[lkey], 'right': right[rkey]}
                        else:
                            new[rkey] = {'left': None, 'right': right[rkey]}    
                else:
                    for rkey in right:
                        new[rkey] = {'left': None, 'right': right[rkey]}    
                    
                    new[lkey] = {'left': left[lkey], 'right': None} 
            yield kmsg         
                    
                     
            
            
        
    