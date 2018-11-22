#!/bin/bash

set -x

java -cp drkafka/target/lib/*:drkafka/target/doctorkafka-0.1.0.jar \
     -Dlog4j.configurationFile=file:./drkafka/config/log4j2.xml   \
     com.pinterest.doctorkafka.DoctorKafkaMain   \
     server drkafka/config/doctorkafka.prod.yaml