#!/usr/bin/env bash

set -x

java -cp drkafka/target/lib/*:drkafka/target/doctorkafka-0.2.4.8.jar \
     -Dlog4j.configurationFile=file:./drkafka/config/log4j2.dev.xml   \
     com.pinterest.doctorkafka.tools.DoctorKafkaActionRetriever   \
     $1 $2 $3 $4 $5 $6
