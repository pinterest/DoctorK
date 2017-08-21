package com.pinterest.doctorkafka;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.util.KafkaUtils;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class KafkaClusterManagerTest {

  /**
   *  Partition(topic = brokerstats, partition = 7, leader = 7016,
   *          replicas = [7017,7011,7018,7016], isr = [7017,7011,7016])
   */
  @Test
  public void getOutOfSyncBrokersTest() throws Exception {
    DoctorKafkaConfig config = new DoctorKafkaConfig("./config/doctorkafka.properties");

    Node leader = new Node(7016, "datakafka07016", 9092);
    Node[] replicas = new Node[]{new Node(7017, "datakafka07017", 9092),
                                 new Node(7011, "datakafka07011", 9092),
                                 new Node(7018, "datakafka07018", 9092),
                                 new Node(7016, "datakafka07016", 9092)};

    Node[] isrs = new Node[]{new Node(7017, "datakafka07017", 9092),
                             new Node(7011, "datakafka07011", 9092),
                             new Node(7016, "datakafka07016", 9092)};

    PartitionInfo partitionInfo = new PartitionInfo("brokerstats", 7, leader, replicas, isrs);
    KafkaClusterManager clusterManager = new KafkaClusterManager("datazk001:2181/testk10", null,
        config.getClusterConfigByName("cluster1"), null, null);
    Set<Node> nodes = KafkaUtils.getNotInSyncBrokers(partitionInfo);
    assertEquals(1, nodes.size());
    assertEquals(replicas[2], nodes.iterator().next());
  }
}
