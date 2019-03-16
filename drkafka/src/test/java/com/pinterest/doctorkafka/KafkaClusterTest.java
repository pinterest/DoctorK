package com.pinterest.doctorkafka;

import static org.junit.jupiter.api.Assertions.*;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.util.OutOfSyncReplica;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class KafkaClusterTest {
  private static final String TOPIC = "test_topic";
  private static final String ZOOKEEPER_URL = "zk001/cluster1";

  /**
   * This test assures that getAlternativeBrokers does not return a many-to-one reassignment
   * We want to make sure that multiple out-of-sync replicas of the same topic partition will not
   * map to the same replacement broker, or duplicate reassigments will happen leading to invalid
   * reassignment plans.
   */
  @Test
  void getAlternativeBrokersDuplicateReassignmentTest() throws Exception{
    DoctorKafkaConfig config = new DoctorKafkaConfig("./config/doctorkafka.properties");
    DoctorKafkaClusterConfig doctorKafkaClusterConfig = config.getClusterConfigByName("cluster1");

     // create histogram maps to mock network stats per topic partition
    ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>> oldBytesInStats = ReplicaStatsManager.bytesInStats;
    ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>> oldBytesOutStats = ReplicaStatsManager.bytesOutStats;

    TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

    ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>> testBytesInStats = new ConcurrentHashMap<>();
    ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>> testBytesOutStats = new ConcurrentHashMap<>();
    ConcurrentMap<TopicPartition, Histogram> testBytesInHistograms = new ConcurrentHashMap<>();
    ConcurrentMap<TopicPartition, Histogram> testBytesOutHistograms = new ConcurrentHashMap<>();
    Histogram inHist = new Histogram(new UniformReservoir());
    Histogram outHist = new Histogram(new UniformReservoir());

    inHist.update(0);
    outHist.update(0);

    testBytesInHistograms.put(topicPartition, inHist);
    testBytesOutHistograms.put(topicPartition, outHist);

    testBytesInStats.put(ZOOKEEPER_URL, testBytesInHistograms);
    testBytesOutStats.put(ZOOKEEPER_URL, testBytesOutHistograms);

    ReplicaStatsManager.bytesInStats = testBytesInStats;
    ReplicaStatsManager.bytesOutStats = testBytesOutStats;

    KafkaCluster kafkaCluster = new KafkaCluster(ZOOKEEPER_URL, doctorKafkaClusterConfig);

    Node[] nodes = new Node[]{
        new Node(0, "test00", 9092),
        new Node(1, "test01", 9092),
        new Node(2, "test02", 9092),
        new Node(3, "test03", 9092),
        new Node(4, "test04", 9092)
    };

    Node leader = nodes[0];
    Node[] replicas = new Node[]{
        nodes[0],
        nodes[1],
        nodes[2]
    };

    Node[] isrs = new Node[]{
        nodes[0]
    };
    PartitionInfo partitionInfo = new PartitionInfo(TOPIC, 0, leader, replicas, isrs);
    OutOfSyncReplica oosReplica = new OutOfSyncReplica(partitionInfo);
    oosReplica.replicaBrokers = Arrays.asList(new Integer[]{0,1,2});


    KafkaBroker[] brokers = new KafkaBroker[]{
        new KafkaBroker(doctorKafkaClusterConfig, 0),
        new KafkaBroker(doctorKafkaClusterConfig, 1),
        new KafkaBroker(doctorKafkaClusterConfig, 2),
        new KafkaBroker(doctorKafkaClusterConfig, 3),
        new KafkaBroker(doctorKafkaClusterConfig, 4)
    };

    // setting the reservedBytesIn for priority queue comparisons
    brokers[4].reserveInBoundBandwidth(topicPartition, 10);
    brokers[4].reserveOutBoundBandwidth(topicPartition, 10);

    PriorityQueue<KafkaBroker> brokerQueue = new PriorityQueue<>();

    for ( KafkaBroker broker : brokers){
      brokerQueue.add(broker);
    }

    int beforeSize = brokerQueue.size();

    /*
     * getMaxBytesIn() and getMaxBytesOut() will return 0 for each broker,
     * so the ordering of the brokers will not change after reassignment
     */
    Map<Integer, KafkaBroker> altBrokers = kafkaCluster.getAlternativeBrokers(brokerQueue, oosReplica);

    // There should be a valid reassignment for this scenario
    assertNotNull(altBrokers);
    // Broker 0 should not be reassigned since it is still in sync
    assertNull(altBrokers.get(0));
    // The reassignment of broker 1 and 2 should be different brokers
    assertNotEquals(altBrokers.get(1), altBrokers.get(2));
    // The broker queue should contain the same amount of brokers
    assertEquals(beforeSize, brokerQueue.size());

    ReplicaStatsManager.bytesInStats = oldBytesInStats;
    ReplicaStatsManager.bytesOutStats = oldBytesOutStats;
  }

}