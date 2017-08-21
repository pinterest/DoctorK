#!/bin/bash
# Start pinshot process inside a docker container with java installed
# Some settings can be overriden by environment variables as shown below

ulimit -n 65536

export SERVICENAME=${SERVICENAME:=kafkaoperator}
export JAVA_MAIN=${JAVA_MAIN:=com.pinterest.doctorkafka.DoctorKafkaMain}
export LOG4J_CONFIG_FILE=${LOG4J_CONFIG_FILE:=/opt/doctorkafka/log4j2.xml}
export CONFIG_FILE=${CONFIG_FILE:=drkafka/config/doctorkafka.properties}
HEAP_SIZE=${HEAP_SIZE:=2G}
NEW_SIZE=${NEW_SIZE:=1G}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PARENT_DIR="$(dirname $DIR)"

LOG_DIR=/var/log/${SERVICENAME}
CP=${PARENT_DIR}:${PARENT_DIR}/*:${PARENT_DIR}/lib/*

exec java -server -Xmx${HEAP_SIZE} -Xms${HEAP_SIZE}  -XX:NewSize=${NEW_SIZE} \
    -XX:MaxNewSize=${NEW_SIZE}  -verbosegc -Xloggc:${LOG_DIR}/gc.log \
    -XX:MetaspaceSize=96m -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=45 -XX:G1HeapRegionSize=16M \
    -XX:MinMetaspaceFreeRatio=60 -XX:MaxMetaspaceFreeRatio=80 \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=50 -XX:GCLogFileSize=6M \
    -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
    -XX:ErrorFile=${LOG_DIR}/jvm_error.log -Dnetworkaddress.cache.ttl=60 -Djava.net.preferIPv4Stack=true \
    -cp ${CP} -Dlog4j.configurationFile=${LOG4J_CONFIG_FILE} -Dserver_config=${CONFIG_FILE} \
    -Dfile.encoding=ISO-8859-1 \
    -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10102 \
    -Dfile.encoding=UTF-8 \
    ${JAVA_MAIN} -config operator/config/doctorkafka.${STAGE_NAME}.properties  \
     -topic brokerstats -zookeeper datazk001:2181/data07  \
     -ostrichport 2052 -tsdhostport localhost:18261 -uptimeinseconds 86400
