'''
Created on Mar 8, 2019

@author: barnwaldo
'''

import sqlite3
import pickle
import time
import traceback
import json
from kafka import KafkaProducer
from collections import UserDict

#################################################################################################################
#################################################################################################################
#################################################################################################################
"""Utility functions for sqlite3 encoding and decoding with binary pickle format"""

def encode(obj):
    '''
    Serialize an object using pickle to a binary format accepted by SQLite.
    '''
    return sqlite3.Binary(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))


def decode(obj):
    '''
    Deserialize pickled objects retrieved from SQLite.
    '''
    return pickle.loads(bytes(obj))

#################################################################################################################
#################################################################################################################
#################################################################################################################

class KStore(UserDict):
    """ Class that creates a dict-like interface for using sqlite as a key/value store loosely based on SqliteDict
        Pickle is used internally to serialize the values. Keys are strings.

        The class KStore is written so that an alternate key/value embedded database can be implemented
        such as sqlite LSM1, RocksDB, LevelDB or Wiredtiger
    """

    def __init__(self, tablename, filename='streams.db', cleartable=True, encode=encode, decode=decode):
        '''Initialize a thread-safe sqlite-backed dictionary. A single file (=database)
            may contain multiple tables - a new connection is made for every table
        
            The `flag` parameter. Exactly one of:
                'c': default mode, open for read/write, creating the db/table if necessary.
                'w': open for r/w, but drop `tablename` contents first (start with empty table)
                'r': open as read-only

            `encode` and `decode` parameters are from SqliteDict and use pickle as a default for the dictionary value (BLOB)
        '''
        self.tablename = tablename
        self.cleartable = cleartable
        self.encode = encode
        self.decode = decode
        try:
            self.conn = sqlite3.connect(filename, isolation_level=None)
            self.conn.execute('pragma journal_mode=wal;')
        except Exception:
            print('EXCEPTION:', self.tablename,"__init__")
            traceback.print_exc()
        self.create_table()

   
    def __str__(self):
        return "Sqlite Table = {}".format(self.tablename)

    
    def __repr__(self):
        return str(self)  # no need of something complex

    
    def __len__(self):
        try:
            GET_MAX = 'SELECT COUNT(*) FROM "{}"'.format(self.tablename)
            self.cursor.execute(GET_MAX)
        except Exception:
            print('EXCEPTION:', self.tablename,"__len__")
            traceback.print_exc()
            return 0
        else:
            result = self.cursor.fetchone()
            return result[0] if result is not None else 0

    
    def __bool__(self):
        try:
            # No elements is False, otherwise True
            GET_MAX = 'SELECT max(rowid) FROM "{}"'.format(self.tablename)
            self.cursor.execute(GET_MAX)
        except Exception:
            print('EXCEPTION:', self.tablename,"__bool__")
            traceback.print_exc()
            return False
        else:    
            result = self.cursor.fetchone()
            return True if result is not None else False
      
    
    def create_table(self):
        try:
            MAKE_TABLE = 'CREATE TABLE IF NOT EXISTS "{}" (key TEXT PRIMARY KEY, value BLOB)'.format(self.tablename)
            self.conn.execute(MAKE_TABLE)
        except Exception:
            print('EXCEPTION:', self.tablename,"create_table")
            traceback.print_exc()
        else:
            self.conn.commit()
            self.cursor = self.conn.cursor()
            if self.cleartable:
                # avoid VACUUM, as it gives "OperationalError: database schema has changed"
                try:
                    CLEAR_ALL = 'DELETE FROM "{}";'.format(self.tablename)
                    self.conn.execute(CLEAR_ALL)
                except Exception:
                    print('EXCEPTION:', self.tablename,"create_table-clear")
                    traceback.print_exc()
                else:
                    self.conn.commit()

    
    def keys(self):
        try:
            GET_KEYS = 'SELECT key FROM "{}" ORDER BY rowid'.format(self.tablename)
            self.cursor.execute(GET_KEYS)
        except Exception:
            print('EXCEPTION:', self.tablename,"keys")
            traceback.print_exc()
            return []
        else:
            keys = self.cursor.fetchall()       
            return [key[0] for key in keys] 

    
    def values(self):
        try:
            GET_VALUES = 'SELECT value FROM "{}" ORDER BY rowid'.format(self.tablename)
            self.cursor.execute(GET_VALUES)
        except Exception:
            print('EXCEPTION:', self.tablename,"values")
            traceback.print_exc()
            return []
        else:
            values = self.cursor.fetchall()       
            return [self.decode(value[0]) for value in values]  

    
    def items(self):
        try:
            GET_ITEMS = 'SELECT key, value FROM "{}" ORDER BY rowid'.format(self.tablename)
            self.cursor.execute(GET_ITEMS)
        except Exception:
            print('EXCEPTION:', self.tablename,"items")
            traceback.print_exc()
            return []
        else:
            rows = self.cursor.fetchall()
            return [(row[0], self.decode(row[1])) for row in rows]
        

    def __contains__(self, key):
        '''
        method not called directly but as 'x in y'
        '''
        try:
            HAS_ITEM = 'SELECT 1 FROM "{}" WHERE key = ?'.format(self.tablename)
            self.cursor.execute(HAS_ITEM, (key,))
        except Exception:
            print('EXCEPTION:', self.tablename,"__contains__")
            traceback.print_exc()
            return False
        else:
            result = self.cursor.fetchone()
            return True if result is not None else False
     
     
    def __getitem__(self, key):
        try:
            GET_ITEM = 'SELECT value FROM "{}" WHERE key = ?'.format(self.tablename)
            self.cursor.execute(GET_ITEM, (key,))
        except Exception:
            print('EXCEPTION:', self.tablename,"__getitem__")
            traceback.print_exc()
            return None
        else:
            result = self.cursor.fetchone()
            if result is None:
                return None
            return self.decode(result[0])
    
    def __setitem__(self, key, value):
        self.conn.execute('BEGIN')
        try:
            UPSERT = 'INSERT INTO {} (key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'.format(self.tablename)
            self.conn.execute(UPSERT, (key, self.encode(value)))        
        except Exception:
            self.conn.rollback()
            print('EXCEPTION:', self.tablename,"__setitem__")
            traceback.print_exc()
        else:
            self.conn.commit()
            
    
    def __delitem__(self, key):
        '''
        call by del mykstore[key]
        '''
        try:
            DEL_ITEM = 'DELETE FROM "{}" WHERE key = ?'.format(self.tablename)
            self.conn.execute(DEL_ITEM, (key,))
        except Exception:
            self.conn.rollback()
            print('EXCEPTION:', self.tablename,"__delitem__")
            traceback.print_exc()
        else:
            self.conn.commit()

    
    def __iter__(self):
        return self.keys()

#################################################################################################################
#################################################################################################################
#################################################################################################################

class KWindow(object):
    '''Class creates time windows for kstreams   
        -- parameters:
                wintime = duration (tumbling, hopping, sliding) or inactivity (session) in sec
                wintype = 'tumbling', 'hopping', 'sliding', 'session
                advance = time in sec for hopping window advance delta
    '''
     
    def __init__(self, wintime, wintype='sliding', advance=0):
 
        self.starttime = time.time()
        self.wintime = wintime
        self.wintype = wintype
        self.advance = advance
        self.windowlist = []
 
    def window(self, stream):
        for kv in stream:
            if kv.time == 0.0:
                kv.time = time.time()
            # update windowed lists for each key based on current time
            current = kv.time
            if self.wintype == 'tumbling':
                if current > self.starttime + self.wintime + self.advance:
                    self.starttime = current
                    self.windowlist.clear()         
                                                                
            elif self.wintype == 'hopping':
                if current > self.starttime + self.wintime + self.advance:
                    self.starttime += self.advance
                    self.windowlist = list(filter(lambda msg: msg.time > self.starttime, self.windowlist))                  
                                            
            elif self.wintype == 'sliding':
                self.windowlist = list(filter(lambda msg: msg.time > current - self.wintime, self.windowlist))
                # print([current - kv.time for kv in self.windowlist]) --> sliding window tested 2019-03-27
                 
            elif self.wintype == 'session':
                if current - self.windowlist[-1].time > self.wintime:
                    self.windowlist.clear()
                         
            else:
                print('No window of type:', self.wintype)
                return
                    
            # add message to windowlist            
            self.windowlist.append(kv)
            kv.val = self.windowlist
            # print(kv.key, kv.val, self.windowlist)
            yield kv


#################################################################################################################
#################################################################################################################
#################################################################################################################

  
class KeyValuePair(object):     
    '''Class for Key Value Pair (also includes all kafka message info for tracking purposes - e.g., exactly once)'''
    
    def __init__(self, key, val, partition=0, offset=0, time=0.0):
        self.key = key
        self.val = val
        self.partition = partition
        self.offset = offset
        self.time = time
        
#################################################################################################################
#################################################################################################################
#################################################################################################################
'''Utility functions for kafka pystreams'''

def serde(x, kvsd):
    '''default serde options'''
    if kvsd == 'int':
        return int(x)
    elif kvsd == 'float':
        return float(x)
    elif kvsd == 'str':
        return x.decode('utf-8')
    elif kvsd == 'json':
        try:
            return json.loads(x.decode('utf-8'))      
        except json.decoder.JSONDecodeError:
            return x.decode('utf-8')
    else:
        # invalid serde type - just return string value
        return x.decode('utf-8')

        
def kproducer(stream, topic, bootstrap_servers, keyserde, valserde):
    '''kafka-python producer'''
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    print(topic, bootstrap_servers, keyserde, valserde)
    for kv in stream:             
        key = ''
        if keyserde == 'json':
            if isinstance(kv.key, dict):
                key = json.dumps(kv.key)
            else:
                key = json.dumps(kv.key.__dict__)
        else:
            key = str(kv.key)
                 
        val = ''
        if valserde == 'json':
            if isinstance(kv.val, dict):
                val = json.dumps(kv.val)
            else:
                val = json.dumps(kv.val.__dict__)
        else:
            val = str(kv.val)
            
        # can add success and error callbacks in future
        # print("kproducer->", key, val)
        producer.send(topic, key=key.encode('utf-8'), value=val.encode('utf-8'))  
    
        
def kqueue_maker(stream, kqueue):
    '''populate (multiprocess) queue from generator stream'''
    for kv in stream:
        kqueue.put(kv)  
        
#################################################################################################################
#################################################################################################################
#################################################################################################################
