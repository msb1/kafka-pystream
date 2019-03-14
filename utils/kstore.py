'''
Created on Mar 8, 2019

@author: barnwaldo

A dict-like interface for using sqlite as a key/value store loosely based on SqliteDict
Pickle is used internally to serialize the values. Keys are strings.

The class KStore is written so that an alternate key/value embedded database can be implemented
such as sqlite LSM1, RocksDB, LevelDB or Wiredtiger

'''

import sqlite3
import pickle
import gevent
import atexit
import time
from collections import UserDict

class DBConnection:
    
    __instance = None
    @staticmethod 
    def getInstance():
        """ Static access method. """
        if DBConnection.__instance == None:
            DBConnection()
        return DBConnection.__instance
 
   
    def __init__(self, filename='streams.db', journal_mode="WAL", commit_interval=1.0):
        """ Virtually private constructor. """
        if DBConnection.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            print("opening Sqlite with file {}".format(filename))
            self.runflag = False
            self.connection = sqlite3.connect(filename)    
            self.connection.execute('PRAGMA journal_mode = {}'.format(journal_mode))
            self.connection.text_factory = str
            self.cursor = self.connection.cursor()
            self.connection.commit()
            self.cursor.execute('PRAGMA synchronous=OFF')
            self.dbthread = gevent.spawn(self.autocommit, commit_interval)
            atexit.register(self.cleanup)
            DBConnection.__instance = self


    def cleanup(self):
        self.runflag = False
        try:
            self.conn.commit()
        except sqlite3.ProgrammingError:
            print("KStore cleanup - cannot commit - database already closed...")
        self.dbthread.join()
        self.dbthread.kill()

    
    def autocommit(self, commit_interval):
        self.runflag = True
        while self.runflag:
            gevent.sleep(commit_interval)
            try:
                self.conn.commit()
            except sqlite3.ProgrammingError:
                print("KStore autocommit() - cannot commit - database is closed...")
                print("autocommit Greenlet will terminate... Restart application")
                self.runflag = False        


    def disconnect(self):
        self.cursor.execute("PRAGMA database_list")
        rows = self.cursor.fetchall()
        for row in rows:
            print("closing {}".format(row[2]))
        try:
            self.conn.commit()
        except sqlite3.ProgrammingError:
            print("disconnect() method - cannot commit - database already closed...")
        self.conn.close()


    def get_tablenames(self):
        '''
        get the names of the tables in an sqlite db as a list
        '''
        self.conn.commit()  
        GET_TABLENAMES = 'SELECT name FROM sqlite_master WHERE type="table"'
        self.cursor = self.conn.execute(GET_TABLENAMES)
        res = self.cursor.fetchall()
        return [name[0] for name in res]


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


class KStore(UserDict):

    def __init__(self, tablename, cleartable=True, encode=encode, decode=decode):
        '''
        Initialize a thread-safe sqlite-backed dictionary. A single file (=database)
        may contain multiple tables so the connect is external to this dictionary and pass as the 'conn'
        parameter
        
        The `flag` parameter. Exactly one of:
          'c': default mode, open for read/write, creating the db/table if necessary.
          'w': open for r/w, but drop `tablename` contents first (start with empty table)
          'r': open as read-only


        Changes are committed on `self.commit()`, self.clear()` and `self.close()'. self.commit() is called by a 
        Greenlet every commit_interval

        `journal_mode` defaults to WAL for performance. Turn to 'OFF' or 'DELETE' if you're experiencing sqlite I/O problems.

        `encode` and `decode` parameters are from SqliteDict and use pickle as a default for the dictionary value (BLOB)

        '''
        self.tablename = tablename
        self.cleartable = cleartable
        self.runflag = False
        self.encode = encode
        self.decode = decode
        self.conn = DBConnection.getInstance().connection
        self.cursor = DBConnection.getInstance().cursor
        self.create_table()

   
    def __str__(self):
        return "Sqlite Table = {}".format(self.tablename)

    
    def __repr__(self):
        return str(self)  # no need of something complex

    
    def __len__(self):
        GET_MAX = 'SELECT COUNT(*) FROM "{}"'.format(self.tablename)
        self.cursor.execute(GET_MAX)
        result = self.cursor.fetchone()
        return result[0] if result is not None else 0

    
    def __bool__(self):
        # No elements is False, otherwise True
        GET_MAX = 'SELECT max(rowid) FROM "{}"'.format(self.tablename)
        self.cursor.execute(GET_MAX)
        result = self.cursor.fetchone()
        return True if result is not None else False
      
    
    def create_table(self):
        MAKE_TABLE = 'CREATE TABLE IF NOT EXISTS "{}" (key TEXT PRIMARY KEY, value BLOB)'.format(self.tablename)
        self.conn.execute(MAKE_TABLE)
        self.conn.commit()
        if self.cleartable:
            # avoid VACUUM, as it gives "OperationalError: database schema has changed"
            CLEAR_ALL = 'DELETE FROM "{}";'.format(self.tablename)  
            try:
                self.conn.commit()
                self.conn.execute(CLEAR_ALL)
                self.conn.commit()
            except sqlite3.ProgrammingError:
                print("KStore clear() - cannot commit - database is closed...")

    
    def keys(self):
        GET_KEYS = 'SELECT key FROM "{}" ORDER BY rowid'.format(self.tablename)
        self.cursor.execute(GET_KEYS)
        keys = self.cursor.fetchall()       
        return [key[0] for key in keys] 

    
    def values(self):
        GET_VALUES = 'SELECT value FROM "{}" ORDER BY rowid'.format(self.tablename)
        self.cursor.execute(GET_VALUES)
        values = self.cursor.fetchall()       
        return [self.decode(value[0]) for value in values]  

    
    def items(self):
        GET_ITEMS = 'SELECT key, value FROM "{}" ORDER BY rowid'.format(self.tablename)
        self.cursor.execute(GET_ITEMS)
        rows = self.cursor.fetchall()
        return [(row[0], self.decode(row[1])) for row in rows]
        

    def __contains__(self, key):
        '''
        method not called directly but as 'x in y'
        '''
        HAS_ITEM = 'SELECT 1 FROM "{}" WHERE key = ?'.format(self.tablename)
        self.cursor.execute(HAS_ITEM, (key,))
        result = self.cursor.fetchone()
        return True if result is not None else False
     
    def __getitem__(self, key):
        GET_ITEM = 'SELECT value FROM "{}" WHERE key = ?'.format(self.tablename)
        self.cursor.execute(GET_ITEM, (key,))
        result = self.cursor.fetchone()
        if result is None:
            return None
        return self.decode(result[0])
    
    def __setitem__(self, key, value):
        UPSERT = 'INSERT INTO {} (key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'.format(self.tablename)
        self.conn.execute(UPSERT, (key, self.encode(value)))        

    
    def __delitem__(self, key):
        '''
        call by del mykstore[key]
        '''
        DEL_ITEM = 'DELETE FROM "{}" WHERE key = ?'.format(self.tablename)
        self.conn.execute(DEL_ITEM, (key,))

    
    def __iter__(self):
        return self.keys()


class KWindow(KStore):
    
    def __init__(self, tablename, windict, cleartable=True, encode=encode, decode=decode):
        super().__init__(self, tablename, cleartable=cleartable, encode=encode, decode=decode)

        self.starttime = time.time()
        self.windowtype = windict['windowtype'] 
        if 'duration' in windict:
            self.duration = windict['duration']  
        if 'advance' in windict:
            self.advance = windict['advance']
        if 'inactivity' in windict:
            self.inactivity = windict['inactivity']


    def __setitem__(self, key, kmsg):
        
                # update windowed lists for each key based on current time
        current = time.time()
        if self.windowtype == 'tumbling':
            if current > self.starttime + self.duration:
                self.starttime = current
                self.clear()
                               
        elif self.windowtype == 'hopping':
            if current > self.starttime + self.advance:
                for k,v in self.items():
                    self[k] = list(filter(lambda msg: msg.time > current - self.duration, v))
                self.starttime = current
                    
        elif self.windowtype == 'sliding':
            for k,v in self.items():
                    self[k] = list(filter(lambda msg: msg.time > current - self.duration, v))
            
        elif self.windowtype == 'session':
            for k,v in self.items():
                if current - self[k][-1].time > self.inactivity:
                    self[k] = []
        else:
            print('No window of type:', self.windowtype)
            return
        
        # add message to window for key
        if key in self:
            self[key].append(kmsg)
        else:                 
            self[key] = [kmsg] 

        UPSERT = 'INSERT INTO {} (key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'.format(self.tablename)
        self.conn.execute(UPSERT, (key, self.encode(self[key])))  


