package com.pinterest.doctorkafka;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;

import org.junit.jupiter.api.Test;

public class KafkaBrokerTest {

  @Test
  public void kafkaBrokerComparatorTest() throws Exception {

    DoctorKafkaConfig config = new DoctorKafkaConfig("./config/doctorkafka.properties");
    DoctorKafkaClusterConfig clusterConfig = config.getClusterConfigByName("cluster1");
    ReplicaStatsManager mockReplicaStatsManager = mock(ReplicaStatsManager.class);

    KafkaBroker a = new KafkaBroker(clusterConfig, mockReplicaStatsManager, 0);
    KafkaBroker b = new KafkaBroker(clusterConfig, mockReplicaStatsManager, 1);

    KafkaBroker.KafkaBrokerComparator comparator = new KafkaBroker.KafkaBrokerComparator();
    assertEquals(0, comparator.compare(a, b));
  }
}
