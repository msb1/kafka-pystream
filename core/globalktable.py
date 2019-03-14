'''
Created on Mar 4, 2019

@author: barnwaldo
'''
from utils.kstore import KStore
from core.kstreambuilder import KStreamCloner, KStreamGenMaker

class GlobalKTable(object):
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
        
        
    def cleanup(self):
        self.runflag = False
        for thread in self.gthreads:
            thread.join()
            thread.kill()   
           
           
    def table(self, stream, kstore):
        queue = KStreamCloner(stream, 1).gqueues[0]
        stream = KStreamGenMaker(queue, kstore=kstore).stream()
        return stream
    
    
        