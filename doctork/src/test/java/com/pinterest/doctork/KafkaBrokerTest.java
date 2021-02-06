package com.pinterest.doctork;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.doctork.config.DoctorKClusterConfig;
import com.pinterest.doctork.config.DoctorKConfig;

import org.junit.jupiter.api.Test;

public class KafkaBrokerTest {

  @Test
  public void kafkaBrokerComparatorTest() throws Exception {

    DoctorKConfig config = new DoctorKConfig("./config/doctork.properties");
    DoctorKClusterConfig clusterConfig = config.getClusterConfigByName("cluster1");
    KafkaCluster kafkaCluster = new KafkaCluster(clusterConfig.getZkUrl(), clusterConfig);

    KafkaBroker a = new KafkaBroker(clusterConfig, kafkaCluster, 0);
    KafkaBroker b = new KafkaBroker(clusterConfig, kafkaCluster, 1);

    KafkaBroker.KafkaBrokerComparator comparator = new KafkaBroker.KafkaBrokerComparator();
    assertEquals(0, comparator.compare(a, b));
  }
}
