package com.pinterest.doctorkafka;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;

import org.junit.jupiter.api.Test;

public class KafkaBrokerTest {
  private static String ZOOKEEPER_URL = "zookeeper01:2181";

  @Test
  public void kafkaBrokerComparatorTest() throws Exception {

    KafkaCluster kafkaCluster = new KafkaCluster(ZOOKEEPER_URL, 1440);

    KafkaBroker a = new KafkaBroker("", kafkaCluster, 0, 0, 0);
    KafkaBroker b = new KafkaBroker("", kafkaCluster, 1, 0, 0);

    KafkaBroker.KafkaBrokerComparator comparator = new KafkaBroker.KafkaBrokerComparator();
    assertEquals(0, comparator.compare(a, b));
  }
}
