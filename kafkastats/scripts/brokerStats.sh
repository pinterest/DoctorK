#!/bin/bash

set -x

#  -verbose
#       -Dorg.apache.logging.log4j.simplelog.StatusLogger.level=TRACE \
#  datakafka01163

java  \
       -cp target/lib/*:target/kafkastats-0.1.0.jar  \
       -Dlog4j.configurationFile=file:./config/log4j2.xml  \
       com.pinterest.doctorkafka.tools.BrokerStatsReader  \
        -zookeeper datazk001:2181/data07\
        -topic brokerstats