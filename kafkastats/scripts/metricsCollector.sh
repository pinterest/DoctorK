#!/bin/bash

set -x

#  -verbose
#       -Dorg.apache.logging.log4j.simplelog.StatusLogger.level=TRACE \

java   -cp target/lib/*:target/kafkastats-0.1.0.jar \
       -Dlog4j.configurationFile=file:./config/log4j2.xml  \
       com.pinterest.doctork.stats.KafkaStatsMain \
         -broker datakafka06013 -jmxport 9999  \
         -topic kafka_test  -zookeeper zookeeper01:2181 \
         -tsdhostport localhost:18126  -ostrichport 2051 \
         -uptimeinseconds 3600  -pollingintervalinseconds 15