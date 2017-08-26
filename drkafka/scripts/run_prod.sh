#!/bin/bash

set -x

java -cp drkafka/target/lib/*:drkafka/target/doctorkafka-0.1.0.jar \
     -Dlog4j.configurationFile=file:./drkafka/config/log4j2.xml   \
     com.pinterest.doctorkafka.DoctorKafkaMain   \
     -config  drkafka/config/doctorkafka.prod.properties \
     -topic brokerstats -zookeeper datazk001:2181/data07  \
     -ostrichport 2052 -tsdhostport localhost:18261 -uptimeinseconds 2000
