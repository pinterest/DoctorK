# Pinterest DoctorKafka

[![Build Status](https://travis-ci.org/pinterest/doctorkafka.svg)](https://travis-ci.org/pinterest/doctorkafka)

DoctorKafka is a service for [Kafka] cluster auto healing and workload balancing.  DoctorKafka can automatically detect broker failure and reassign the workload on the failed nodes to other nodes. DoctorKafka can also perform load balancing based on topic partitions's network usage, and makes sure that broker network usage does not exceed the defined settings. DoctorKafka sends out alerts when it is not confident on taking actions.

#### Features   

 * Automated cluster healing by moving partitions on failed brokers to other brokers
 * Workload balancing among brokers
 * Replica count adjustment based on available brokers
 * Centralized management of multiple kafka clusters 

#### Detailed design

Design details are available in [docs/DESIGN.md](docs/DESIGN.md).

## Setup Guide

##### Get DoctorKafka code
```sh
git clone [git-repo-url] doctorkafka
cd doctorkafka
```

##### Build kafka stats collector and deployment it to kafka brokers 

```aidl
mvn package -pl kafkastats -am 
```

Kafkastats is a kafka broker stats collector that runs on kafka brokers and reports broker stats 
to some kafka topic based on configuration. The following is the kafkastats usage.

```usage: KafkaMetricsCollector
    -jmxport <kafka jmx port number>   kafka process jmx port number
    -kafka_config <arg>                kafka server properties file path
    -ostrichport <arg>                 ostrich port
    -pollingintervalinseconds <arg>    kafka broker stats polling interval in seconds
    -topic <arg>                       the kafka topic that metric messages are sent to
    -tsdhostport <arg>                 tsd host and port for tsdb
    -uptimeinseconds <arg>             how often kafkastats should restart itself. 
    -zookeeper <arg>                   zookeeper url for the metrics topic
```

The following is a sample command line for running kafkastats collector:

```
java -server -cp /opt/kafkastats:/opt/kafkastats/*:/opt/kafkastats/lib/*   -Dlog4j.configuration= com.pinterest.doctorkafka.stats.KafkaStatsMain  -jmxport 9999 -topic brokerstats -zookeeper zookeeper001:2181/cluster1  -tsdhostport localhost:18126 -ostrichport 2051 -uptimeinseconds 3600  -pollingintervalinseconds 60 -kafka_config /etc/kafka/server.properties
```

Using the above command as an example, after the kafkastats process is up, we can check the process stats using ```curl -s ``` command, and view the logs under /var/log/kafkastats. 

```aidl
curl -s localhost:2051/stats.txt
```

The following is a sample upstart scripts for automatically restarting kafkastats if it fails:

```description "KafkaStats"
   start on runlevel [2345]
   respawn
   respawn limit 20 5
   
   env NAME=kafkastats
   env JAVA_HOME=/usr/lib/jvm/java-8-oracle
   env STATSCOLLECTOR_HOME=/opt/kafkastats
   env LOG_DIR=/var/log/kafkastats
   env HOSTNAME=$(hostname)
   
   script
       DAEMON=$JAVA_HOME/bin/java
       CLASSPATH=$STATSCOLLECTOR_HOME:$STATSCOLLECTOR_HOME/*:$STATSCOLLECTOR_HOME/lib/*
       DAEMON_OPTS="-server -Xmx800M -Xms800M -verbosegc -Xloggc:$LOG_DIR/gc.log \
       -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=20 -XX:GCLogFileSize=20M \
       -XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:G1ReservePercent=10 -XX:ConcGCThreads=4 \
       -XX:ParallelGCThreads=4 -XX:G1HeapRegionSize=8m -XX:InitiatingHeapOccupancyPercent=70 \
       -XX:ErrorFile=$LOG_DIR/jvm_error.log \
       -cp $CLASSPATH"
       exec $DAEMON $DAEMON_OPTS -Dlog4j.configuration=${LOG_PROPERTIES} \
                    com.pinterest.kafka.stats.KafkaStatsMain \
                    -jmxport 9999  -topic brokerstats  \
                    -zookeeper zookeeper001:2181/cluster1 \
                    -tsdhostport localhost:18126 -ostrichport 2051 \
                    -uptimeinseconds 3600 -pollingintervalinseconds 60 \
                    -kafka_config  /etc/kafka/server.properties
```


##### Customize doctorkafka configuration parameters

Edit `drkafka/config/*.properties` files to specify parameters describing the environment. Those files contain comments describing the meaning of individual parameters.


#### Create and install jars

```
mvn package -pl drkafka -am 
```

```sh
mvn package
mkdir ${DOCTORKAFKA_INSTALL_DIR} # directory to place Secor binaries in.
tar -zxvf target/doctorkafka-0.1.0-bin.tar.gz -C ${DOCTORKAFKA_INSTALL_DIR}
```

##### Run DoctorKafka
```sh
cd ${DOCTORKAFKA_INSTALL_DIR}

java -server -cp lib/*:doctorkafka-0.1.0.jar  com.pinterest.doctorkafka.DoctorKafkaMain \
  -config drkafka/config/doctorkafka.prod.properties -topic brokerstats  \
  -zookeeper zookeeper001:2181/cluster1 -ostrichport 2052  \
  -tsdhostport localhost:18261 -uptimeinseconds 86400
```

The DoctorKafka process will respond to the SIGHUP signal to immediately heal and balance the clusters.

##### Customize configuration parameters
Edit `src/drkafka/config/*.properties` files to specify parameters describing the environment. 
Those files contain comments describing the meaning of individual parameters.


## Tools
DoctorKafka comes with a number of tools implementing interactions with the environment.

##### Cluster Load Balancer

```bash
cd ${DOCTORKAFKA_INSTALL_DIR}
java8 -Dlog4j.configurationFile=file:./log4j2.xml  -cp  lib/*:doctorkafka-0.1.0.jar \
      com.pinterest.doctorkafka.tools.ClusterLoadBalancer \
      -brokerstatstopic  brokerstats -brokerstatszk  zookeeper001:2181/cluster1  \
      -clusterzk  zookeeper001:2181,zookeeper002:2181,zookeeper003:2181/cluster2 \
      -config  ./drkafka/config/doctorkafka.prod.properties
```
Cluster load balancer balances the workload among brokers to make sure the broker network
usage does not exceed the threshold. 


## Maintainers
  * [Yu Yang](https://github.com/yuyang08)

## Contributors

## License

DoctorKafka is distributed under [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

[Kafka]:http://kafka.apache.org/
[Ostrich]: https://github.com/twitter/ostrich
[OpenTSDB]: http://opentsdb.net/
[statsD]: https://github.com/etsy/statsd/
