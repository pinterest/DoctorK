#!/bin/bash

set -x

java -cp doctork/target/lib/*:doctork/target/doctork-0.1.0.jar \
     -Dlog4j.configurationFile=file:./doctork/config/log4j2.xml   \
     com.pinterest.doctork.DoctorKMain   \
     server doctork/config/doctork.prod.yaml