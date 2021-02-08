#!/bin/bash

set -x

java8 -cp lib/*:kafkaoperator-0.2.3.jar  -Dlog4j.configurationFile=file:./log4j2.xml  \
      com.pinterest.doctork.DoctorKMain server doctork.app.yaml