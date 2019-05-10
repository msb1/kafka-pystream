'''
Created on Mar 22, 2019

@author: barnwaldo

Word count Kafka Streams example - migrated from Java to Python pytreams
'''

from core.kbuilder import KStreamBuilder, KStream
from core.kutils import KeyValuePair

# config info for Kafka broker
config = {'bootstrap.servers': '192.168.21.3:9092', 'group.id': 'barnwaldo', 'session.timeout.ms': 6000}
topics = 'word'

if __name__ == '__main__':
    
    # define builder for streams/table
    builder = KStreamBuilder(topics, config, keyserde='str', valserde='str')
    # initialize Kstream with builder
    textlines = KStream(builder.builder)

    # stream with splitting text, flattening and grouping - use Python lambdas in pystreams methods
    textlines.flatMapValues(lambda text: text.lower().split()).groupBy(lambda kv: KeyValuePair(kv.val, kv.val))
    # change stream to table with count on key - materialize is name of db table
    wordcounts = textlines.count(materialized='wordcounter')
    
    # change table back to stream and then send output to Kafka topic
    wordcounts.toStream().to('counts', config, keyserde='str', valserde='str')

    # Note that KStream can either be initialized with a KStreambuilder or a stream (generator) with a Kafka Producer