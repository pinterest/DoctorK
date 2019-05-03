package com.pinterest.doctorkafka;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;

import org.junit.jupiter.api.Test;

public class KafkaBrokerTest {

  @Test
  public void kafkaBrokerComparatorTest() throws Exception {

    DoctorKafkaConfig config = new DoctorKafkaConfig("./config/doctorkafka.properties");
    DoctorKafkaClusterConfig clusterConfig = config.getClusterConfigByName("cluster1");
    KafkaCluster kafkaCluster = new KafkaCluster(clusterConfig.getZkUrl(), clusterConfig);

    KafkaBroker a = new KafkaBroker(clusterConfig, kafkaCluster, 0);
    KafkaBroker b = new KafkaBroker(clusterConfig, kafkaCluster, 1);

    KafkaBroker.KafkaBrokerComparator comparator = new KafkaBroker.KafkaBrokerComparator();
    assertEquals(0, comparator.compare(a, b));
  }
}
