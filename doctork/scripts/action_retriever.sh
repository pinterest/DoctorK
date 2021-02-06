#!/usr/bin/env bash

set -x

java -cp doctork/target/lib/*:doctork/target/doctork-0.2.4.10.jar \
     -Dlog4j.configurationFile=file:./doctork/config/log4j2.dev.xml   \
     com.pinterest.doctork.tools.DoctorKActionRetriever   \
     $1 $2 $3 $4 $5 $6
