#!/bin/bash

set -x


java8 -cp lib/*:kafkaoperator-0.2.2.jar  -Dlog4j.configurationFile=file:./log4j2.xml  \
      com.pinterest.doctorkafka.DoctorKafkaMain   \
      -topic brokerstats -zookeeper datazk001:2181/data07  \
      -ostrichport 2052 -tsdhostport localhost:18261 -uptimeinseconds 86400  \
      -config doctorkafka.prod.properties

