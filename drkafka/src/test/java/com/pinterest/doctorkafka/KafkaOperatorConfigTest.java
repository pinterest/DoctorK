package com.pinterest.doctorkafka;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;

import org.junit.jupiter.api.Test;

public class KafkaOperatorConfigTest {

  @Test
  public void testKafakOperatorConfig() throws Exception {
    DoctorKafkaConfig config = new DoctorKafkaConfig("./config/doctorkafka.properties");

    assertEquals("brokerstats", config.getBrokerStatsTopic());
    assertEquals(2052, config.getOstrichPort());
    assertEquals(86400, config.getRestartIntervalInSeconds());

    DoctorKafkaClusterConfig clusterConfig = config.getClusterConfigByName("cluster1");
    assertEquals(true, clusterConfig.dryRun());
    assertEquals("zookeeper001:2181,zookeeper002:2181,zookeeper003:2181/cluster1",
        clusterConfig.getZkUrl());

    assertEquals(35.0, clusterConfig.getNetworkInLimitInMb());
    assertEquals(80.0, clusterConfig.getNetworkOutLimitInMb());
  }
}