# pystreams-kafka
#### Kafka Streams DSL for Python (Open Source)
#### Notes on current status:
<ol>
<li>Expect first release in June 2019</li>
<li>Testing and debug in progress</li>
<li>Working examples of Confluent Java Test programs: anomaly detection, word count and global tables in test folder of this repo
<li>kafka-python is used in current version since librdkafka used in confluent-kafka-python just went through major upgrades (will likely migrate back to this kafka driver in the future)</li>
<li>Using sqlite3 with upsert as a key value store. Code is written so that other key value stores can be readily inserted.
 <ul>
  <li>python-rocksdb appears to now have a new owner/maintener - will monitor this as a potential replacement. before this, pyrocksdb was not supported since 2015</li>
 <li>python-leveldb is no longer maintained but does work </li>
 <li>BerkeleyDB is well maintained but do not want to venture into new Oracle licensing nonsense</li>
 <li>WiredTiger is potential solution for very fast key value store. WiredTiger has been acquired by MongoDB and is now their default backend and therefore will be well maintained. The python wrapper for WiredTiger has not been maintained for some time and I might work on this in the future if none of the other possibilities pan out.</li>
 <li>Note sqlite3 is very stable and therefore a good choice for initial pystreams-kafka development</li>
 </ul>
 </li>
 </ol>

