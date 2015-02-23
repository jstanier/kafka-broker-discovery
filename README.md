# kafka-broker-discovery [![Build Status](https://travis-ci.org/jstanier/kafka-broker-discovery.svg?branch=master)](https://travis-ci.org/jstanier/kafka-broker-discovery) [![Coverage Status](https://coveralls.io/repos/jstanier/kafka-broker-discovery/badge.svg)](https://coveralls.io/r/jstanier/kafka-broker-discovery)

Discover [Apache Kafka](http://kafka.apache.org/) brokers from Zookeeper. 

Typically you initialise Kafka `Producer` objects by providing a connection String for a subset of Kafka brokers in your cluster; e.g. `kafka1:2222,kafka2:2222`. From here, topic, partition and replica information is used to bootstrap the connection to the cluster.

If your application is already connecting to Zookeeper, then this code discovers a subset of Kafka brokers for you automatically.

# Usage

```java
String zookeeperHost = "zookeeper1";
String zookeeperPort = "4444";
KafkaBrokerDiscoverer discoverer = new KafkaBrokerDiscoverer(zookeeperHost, zookeeperPort);
discoverer.getConnectionString();
discoverer.close();
```
