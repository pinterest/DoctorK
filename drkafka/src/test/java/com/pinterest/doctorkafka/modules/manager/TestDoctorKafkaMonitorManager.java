package com.pinterest.doctorkafka.modules.manager;

import static org.junit.jupiter.api.Assertions.*;

import com.pinterest.doctorkafka.modules.action.cluster.kafka.ZkUtilReassignPartition;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TestDoctorKafkaMonitorManager {

  @Test
  void testGetMonitor() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("nonmonitor.class", ZkUtilReassignPartition.class.getCanonicalName());
    configMap.put("goodmonitor.class","com.pinterest.doctorkafka.modules.monitor.cluster.MaintenanceChecker");
    Configuration config = new MapConfiguration(configMap);
    MonitorManager monitorManager = new DoctorKafkaMonitorManager(config);

    try{
      monitorManager.getMonitor("nonmonitor", null);
      fail("Should not pass since module is not a monitor");
    } catch (Exception e){
      assertTrue(e instanceof ClassCastException);
    }

    try {
      monitorManager.getMonitor("goodmonitor", null);
    } catch (Exception e){
      fail("Should not fail as long as MaintenanceChecker class is in classpath");
    }

  }
}