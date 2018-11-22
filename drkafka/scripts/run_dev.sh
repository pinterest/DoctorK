#!/bin/bash

set -x

java -cp drkafka/target/lib/*:drkafka/target/doctorkafka-0.2.4.jar \
     -Dlog4j.configurationFile=file:./drkafka/config/log4j2.dev.xml   \
     com.pinterest.doctorkafka.DoctorKafkaMain server drkafka/config/doctorkafka.dev.yaml