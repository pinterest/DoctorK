#!/bin/bash

set -x

#  -verbose
#       -Dorg.apache.logging.log4j.simplelog.StatusLogger.level=TRACE \
#  datakafka01163

java   -cp target/lib/*:target/kafkastats-0.1.0.jar \
       -Dlog4j.configurationFile=file:./config/log4j2.xml  \
       com.pinterest.doctorkafka.stats.KafkaStatsMain \
         -broker datakafka03071 -jmxport 9999  \
         -topic kafka_test  -zookeeper datazk001:2181/testk10 \
         -tsdhostport localhost:18126  -ostrichport 2051 \
         -uptimeinseconds 3600  -pollingintervalinseconds 15


