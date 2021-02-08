#!/bin/bash

set -x 

#java -cp target/lib/*:target/metrics_collector-0.1-SNAPSHOT.jar \
#     -Dlog4j2.configuration=file:./config/log4j2.xml \
#     com.pinterest.kafka.metricscollector.MetricsFetcher $1 $2 $3 $4 $5 $6

#  datakafka01163
#

java -cp target/lib/*:target/kafkastats-0.1-SNAPSHOT.jar \
     -Dlog4j.configurationFile=file:./config/log4j2.xml \
     com.pinterest.doctork.tools.MetricsFetcher \
     -host $(hostname) -port 9999 \
     -metric $1
