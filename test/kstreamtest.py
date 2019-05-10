'''
Created on Feb 22, 2019

@author: Barnwaldo
'''
import numpy as np
from core.kbuilder import KStreamBuilder, KStream
from core.kutils import KeyValuePair

                             
config = {'bootstrap.servers': 'localhost:9092', 'group.id': 'barnwaldo', 'session.timeout.ms': 6000}
# config = {'bootstrap.servers': '192.168.21.3:9092', 'group.id': 'barnwaldo', 'session.timeout.ms': 6000}
topics = 'endpt00'
                        

       
func1 = (lambda kv: KeyValuePair(kv.key / 10, kv.val if kv.val == ">>> stop message <<<" \
          else (kv.val['sensor1'] + kv.val['sensor2'] + kv.val['sensor3'] + kv.val['sensor4'] + kv.val['sensor5']) /5))

func2 = (lambda kv: KeyValuePair(kv.key, kv.val if kv.val == ">>> stop message <<<" \
          else (kv.val['sensor1'] + kv.val['sensor2'] + kv.val['sensor3'] + kv.val['sensor4'] + kv.val['sensor5']) /5))


pred1 = (lambda kv: kv.key < 4)
pred2 = (lambda kv: kv.key < 7)
pred3 = (lambda kv: True)

predicates = [pred1, pred2, pred3]

def flatMapValues1(val):
    flatlist = [-1, -1, -1]
    try:
        sensorVals = [val['sensor1'], val['sensor2'], val['sensor3'], val['sensor4'], val['sensor5']]
        flatlist = [np.average(sensorVals), np.std(sensorVals), np.log(np.sum(sensorVals))]
    except:
        pass
    return flatlist

def flatMap1(kv):
    flatlist = [KeyValuePair(0, -1), KeyValuePair(1, -1), KeyValuePair(2, -1)]
    val = kv.val
    try:
        sensorVals = [val['sensor1'], val['sensor2'], val['sensor3'], val['sensor4'], val['sensor5']]
        flatlist = [KeyValuePair(0, np.average(sensorVals)), KeyValuePair(1, np.std(sensorVals)), KeyValuePair(2, np.log(np.sum(sensorVals)))]
    except:
        pass
    return flatlist

def foreach1A(kv):
    if kv.key == 0:
        print(str(kv.key) + "--avg:    ", kv.val)
    elif kv.key == 1:
        print(str(kv.key) + "--stddev: ", kv.val)
    elif kv.key == 2:
        print(str(kv.key) + "--logsum: ", kv.val)
    else:
        print('Key not recognized...')  
        
def foreach1(kv):
    if kv.key == 0:
        print("1--avg:    ", kv.val)
    elif kv.key == 1:
        print("1--stddev: ", kv.val)
    elif kv.key == 2:
        print("1--logsum: ", kv.val)
    else:
        print('Key not recognized...', kv.key) 
        
def foreach2(kv):
    if kv.key == 0:
        print("2--avg:    ", kv.val)
    elif kv.key == 1:
        print("2--stddev: ", kv.val)
    elif kv.key == 2:
        print("2--logsum: ", kv.val)
    else:
        print('Key not recognized...', kv.key) 

def foreach3(kv):
    if kv.key == 0:
        print("3--avg:    ", kv.val)
    elif kv.key == 1:
        print("3--stddev: ", kv.val)
    elif kv.key == 2:
        print("3--logsum: ", kv.val)
    else:
        print('Key not recognized...', kv.key) 

def peek1(kv):
    print("key: {} -- value: {}".format(kv.key, kv.val))
    return kv


def stream1(builder):
    for kv in builder.stream():
        if kv == None:
            continue
        print(kv.key, kv.val)
            
                            
if __name__ == '__main__': 
          
    builder = KStreamBuilder(topics, config, keyserde='int', valserde='json')
    mystream = KStream(builder.builder)
    # --> printer test
    # mystream.printer()
    # --> map test with func to printer
    # mystream.map(func1).printer()
    # --> map test with embedded lambda and func to printer
    # mystream.map((lambda kv: KeyValuePair(5 * kv.key, kv.val))).map(func1)
    # --> filter with prior mapping
    # mystream.filter((lambda kv: kv.key < 2.0)).printer()
    # --> filterNot with prior mapping
    # mystream.filterNot((lambda kv: kv.key < 2.0)).printer()
    # --> flatMapValues 
    # mystream.map((lambda kv: KeyValuePair(5 * kv.key, kv.val))).flatMapValues(flatMapValues1).printer()
    # mystream.map((lambda kv: KeyValuePair(5 * kv.key, kv.val))).flatMap(flatMap1).foreach(foreach1A)
    # --> branch
    # newstreams = mystream.branch(predicates)     
    # newstreams[0].printer()
    # newstreams[1].printer()
    # newstreams[2].printer()
    
    # newstreams[0].flatMap(flatMap1).foreach(foreach1)
    # newstreams[1].flatMap(flatMap1).foreach(foreach2)
    # newstreams[2].flatMap(flatMap1).foreach(foreach3)
    mystream.peeker(peek1)
    mystream.map((lambda kv: KeyValuePair(5 * kv.key, kv.val)))
    mystream.peeker(peek1)
    mystream.map(func1).printer()
    
        

    


    