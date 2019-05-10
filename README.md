# pystreams-kafka
#### Kafka Streams DSL for Python (Open Source)
<br/>
#### Notes on current status
1. Expect first release in June 2019
2. Testing and debug in progress
3. Working examples of Confluent Java Test programs: anomaly detection, word count and global tables in test folder of this repo
4. kafka-python is used in current version since librdkafka used in confluent-kafka-python just went through major upgrades (will likely migrate back to this kafka driver in the future)
5. Using sqlite3 with upsert as a key value store. Code is written so that other key value stores can be readily inserted. 
..* python-rocksdb appears to now have a new owner/maintener - will monitor this as a potential replacement. before this, pyrocksdb was not supported since 2015
..* python-leveldb is no longer maintained but does work 
..* BerkeleyDB is well maintained but do not want to venture into new Oracle licensing nonsense
..* WiredTiger is potential solution for very fast key value store. WiredTiger has been acquired by MongoDB and is now their default backend and therefore will be well maintained. The python wrapper for WiredTiger has not been maintained for some time and I might work on this in the future if none of the other possibilities pan out.
..* Note sqlite3 is very stable and therefore a good choice for initial pystreams-kafka development

