'''
Created on Feb 27, 2019

@author: barnwaldo
'''

import json


class KeyValuePair(object):
    def __init__(self, key, val):
        self.key = key
        self.val = val

    
class KMessage(object):     
    def __init__(self, key, val):
        self.key = key
        self.val = val
        self.partition = 0
        self.offset = 0
        self.time = 0.0


class KTableStream(object):
    def __init__(self, stream, kstore):
        self.stream = stream
        self.tablename = kstore


def serde(x, kvsd):
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

        
    
    
        
    
    
        