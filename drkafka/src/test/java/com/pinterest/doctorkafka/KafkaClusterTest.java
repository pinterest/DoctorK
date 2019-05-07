package com.pinterest.doctorkafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.util.OutOfSyncReplica;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

class KafkaClusterTest {
  private static final String TOPIC = "test_topic";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  private static final String[] LOCALITIES = new String[]{"LOCALITY_0","LOCALITY_1","LOCALITY_2"};
  private static final Node[] nodes = new Node[]{
      new Node(0, "test00", 9092),
      new Node(1, "test01", 9092),
      new Node(2, "test02", 9092),
      new Node(3, "test03", 9092),
      new Node(4, "test04", 9092),
      new Node(5, "test05", 9092),
      new Node(6, "test06", 9092)
  };
  private static final String CLUSTER_NAME = "cluster1";
  private static DoctorKafkaClusterConfig doctorKafkaClusterConfig;
  private static String zookeeper_url;
  private static KafkaCluster kafkaCluster;

  @BeforeAll
  static void setup() throws Exception {
    DoctorKafkaConfig config = new DoctorKafkaConfig("./config/doctorkafka.properties");
    doctorKafkaClusterConfig = config.getClusterConfigByName(CLUSTER_NAME);
    zookeeper_url = doctorKafkaClusterConfig.getZkUrl();
    kafkaCluster = new KafkaCluster(zookeeper_url, doctorKafkaClusterConfig);
  }

  /**
   * This test assures that getAlternativeBrokers does not return a many-to-one reassignment
   * We want to make sure that multiple out-of-sync replicas of the same topic partition will not
   * map to the same replacement broker, or duplicate reassigments will happen leading to invalid
   * reassignment plans.
   */
  @Test
  void getAlternativeBrokersDuplicateReassignmentTest() throws Exception {
    TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

    KafkaCluster spyKafkaCluster = spy(kafkaCluster);
    doReturn(1L).when(spyKafkaCluster).getMaxBytesIn(topicPartition);
    doReturn(0L).when(spyKafkaCluster).getMaxBytesOut(topicPartition);

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
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 0),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 1),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 2),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 3),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 4)
    };

    Set<TopicPartition> replicaSet = new HashSet<>();
    replicaSet.add(topicPartition);

    for ( Node node : replicas ) {
      brokers[node.id()].setFollowerReplicas(replicaSet);
    }

    brokers[leader.id()].setLeaderReplicas(replicaSet);

    // setting the reservedBytesIn for priority queue comparisons
    brokers[4].reserveBandwidth(TOPIC_PARTITION, 10, 10);

    PriorityQueue<KafkaBroker> brokerQueue = new PriorityQueue<>();

    brokerQueue.addAll(Arrays.asList((brokers)));

    int beforeSize = brokerQueue.size();

    double inBoundReq = spyKafkaCluster.getMaxBytesIn(topicPartition);
    double outBoundReq = spyKafkaCluster.getMaxBytesOut(topicPartition);
    int preferredBroker = oosReplica.replicaBrokers.get(0);

    /*
     * getMaxBytesIn() and getMaxBytesOut() will return 0 for each broker,
     * so the ordering of the brokers will not change after reassignment
     */
    Map<Integer, KafkaBroker> altBrokers = kafkaCluster.getAlternativeBrokers(
        brokerQueue,
        oosReplica,
        inBoundReq,
        outBoundReq,
        preferredBroker
    );

    verify(spyKafkaCluster, atLeast(2)).getMaxBytesIn(topicPartition);
    verify(spyKafkaCluster, atLeast(2)).getMaxBytesOut(topicPartition);

    // There should be a valid reassignment for this scenario
    assertNotNull(altBrokers);
    // Broker 0 should not be reassigned since it is still in sync
    assertNull(altBrokers.get(0));
    // The reassignment of broker 1 and 2 should be different brokers
    assertNotEquals(altBrokers.get(1), altBrokers.get(2));
    // The broker queue should contain the same amount of brokers
    assertEquals(beforeSize, brokerQueue.size());
  }

  @Test
  void testLocalityAwareReassignments() throws Exception {
    TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

    KafkaCluster spyKafkaCluster = spy(kafkaCluster);
    doReturn(1L).when(spyKafkaCluster).getMaxBytesIn(topicPartition);
    doReturn(0L).when(spyKafkaCluster).getMaxBytesOut(topicPartition);

    Node leader = nodes[0];
    Node[] replicas = new Node[]{
        nodes[0],
        nodes[1],
        nodes[2],
    };

    Node[] isrs = new Node[]{
        nodes[0]
    };
    PartitionInfo partitionInfo = new PartitionInfo(TOPIC, 0, leader, replicas, isrs);
    OutOfSyncReplica oosReplica = new OutOfSyncReplica(partitionInfo);
    oosReplica.replicaBrokers = Arrays.asList(new Integer[]{0,1,2});


    KafkaBroker[] brokers = new KafkaBroker[]{
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 0),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 1),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 2),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 3),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 4),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 5),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 6)
    };

    for ( KafkaBroker broker : brokers ){
      BrokerStats bs = new BrokerStats();
      bs.setTimestamp(System.currentTimeMillis());
      broker.setLatestStats(bs);
      kafkaCluster.brokers.put(broker.getId(), broker);
    }

    Set<TopicPartition> replicaSet = new HashSet<>();
    replicaSet.add(topicPartition);

    for ( Node node : replicas ) {
      brokers[node.id()].setFollowerReplicas(replicaSet);
    }

    brokers[leader.id()].setLeaderReplicas(replicaSet);

    // a list of assignments of localities to brokers with the same size as LOCALITIES
    List<List<Integer>> testLocalityAssignments = Arrays.asList(
        Arrays.asList(0, 3),
        Arrays.asList(1, 2, 4, 5),
        Arrays.asList(6)
    );

    for (int localityId = 0; localityId < testLocalityAssignments.size(); localityId++) {
      for(int brokerId : testLocalityAssignments.get(localityId)){
        brokers[brokerId].setRackId(LOCALITIES[localityId]);
      }
    }

    // setting the reservedBytesIn for priority queue comparisons
    brokers[4].reserveBandwidth(TOPIC_PARTITION, 10, 10);
    brokers[5].reserveBandwidth(TOPIC_PARTITION, 5, 5);

    Map<String, PriorityQueue<KafkaBroker>> brokerLocalityMap =
        kafkaCluster.getBrokerQueueByLocality();

    for ( int localityId = 0; localityId < testLocalityAssignments.size(); localityId++) {
      Collection<Integer> expectedAssignments = testLocalityAssignments.get(localityId);
      Collection<KafkaBroker> actualAssignments = brokerLocalityMap.get(LOCALITIES[localityId]);
      // size check
      assertEquals(expectedAssignments.size(),actualAssignments.size());
      // element check
      assertTrue(actualAssignments
          .stream()
          .map(broker -> broker.getId())
          .collect(Collectors.toList())
          .containsAll(expectedAssignments));
    }

    double inBoundReq = spyKafkaCluster.getMaxBytesIn(oosReplica.topicPartition);
    double outBoundReq = spyKafkaCluster.getMaxBytesOut(oosReplica.topicPartition);
    int preferredBroker = oosReplica.replicaBrokers.get(0);

    // Test getAlternativeBrokersByLocality,
    // brokers 1 & 2 are out of sync
    // brokers 3 & 6 should be skipped even though they have the least utilization since they are not
    // within the same locality as brokers 1 & 2

    Map<Integer, KafkaBroker> localityReassignments =
        kafkaCluster.getAlternativeBrokersByLocality(
            brokerLocalityMap,
            oosReplica,
            inBoundReq,
            outBoundReq,
            preferredBroker
        );

    verify(spyKafkaCluster,atLeast(2)).getMaxBytesIn(topicPartition);
    verify(spyKafkaCluster,atLeast(2)).getMaxBytesIn(topicPartition);
    assertEquals(2, localityReassignments.size());
    assertEquals(brokers[5], localityReassignments.get(1));
    assertEquals(brokers[4], localityReassignments.get(2));

    // check if the invariants are maintained after reassignment
    for ( int localityId = 0; localityId < testLocalityAssignments.size(); localityId++) {
      Collection<Integer> expectedAssignments = testLocalityAssignments.get(localityId);
      Collection<KafkaBroker> actualAssignments = brokerLocalityMap.get(LOCALITIES[localityId]);
      // size check
      assertEquals(expectedAssignments.size(),actualAssignments.size());
      // element check
      assertTrue(actualAssignments
          .stream()
          .map(broker -> broker.getId())
          .collect(Collectors.toList())
          .containsAll(expectedAssignments));
    }

  }

  @Test
  void testNonLocalityAwareReassignments() throws Exception {
    TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

    KafkaCluster spyKafkaCluster = spy(kafkaCluster);
    doReturn(1L).when(spyKafkaCluster).getMaxBytesIn(topicPartition);
    doReturn(0L).when(spyKafkaCluster).getMaxBytesOut(topicPartition);

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
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 0),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 1),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 2),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 3),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 4),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 5),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 6)
    };

    for ( KafkaBroker broker : brokers ){
      BrokerStats bs = new BrokerStats();
      bs.setTimestamp(System.currentTimeMillis());
      broker.setLatestStats(bs);
      kafkaCluster.brokers.put(broker.getId(), broker);
    }

    Set<TopicPartition> replicaSet = new HashSet<>();
    replicaSet.add(topicPartition);

    for ( Node node : replicas ) {
      brokers[node.id()].setFollowerReplicas(replicaSet);
    }

    brokers[leader.id()].setLeaderReplicas(replicaSet);

    // setting the reservedBytesIn for priority queue comparisons
    brokers[4].reserveBandwidth(TOPIC_PARTITION, 10, 10);
    brokers[5].reserveBandwidth(TOPIC_PARTITION, 5, 5);
    brokers[6].reserveBandwidth(TOPIC_PARTITION, 2, 2);

    Map<String, PriorityQueue<KafkaBroker>> brokerLocalityMap =
        kafkaCluster.getBrokerQueueByLocality();

    assertEquals(brokers.length, brokerLocalityMap.get(null).size());
    assertTrue(brokerLocalityMap.get(null).containsAll(Arrays.asList(brokers)));

    double inBoundReq = spyKafkaCluster.getMaxBytesIn(oosReplica.topicPartition);
    double outBoundReq = spyKafkaCluster.getMaxBytesOut(oosReplica.topicPartition);
    int preferredBroker = oosReplica.replicaBrokers.get(0);

    // Test getAlternativeBrokersByLocality,
    // brokers 1 & 2 are out of sync
    // brokers 3 & 6 are used because they have the lowest utilization

    Map<Integer, KafkaBroker> localityReassignments =
        kafkaCluster.getAlternativeBrokersByLocality(
            brokerLocalityMap,
            oosReplica,
            inBoundReq,
            outBoundReq,
            preferredBroker
        );

    verify(spyKafkaCluster,atLeast(2)).getMaxBytesIn(topicPartition);
    verify(spyKafkaCluster,atLeast(2)).getMaxBytesIn(topicPartition);
    assertEquals(2, localityReassignments.size());
    assertEquals(brokers[3], localityReassignments.get(1));
    assertEquals(brokers[6], localityReassignments.get(2));
  }

  @Test
  void testLocalityAwareReassignmentsFailure() throws Exception {
    TopicPartition topicPartition = new TopicPartition(TOPIC, 0);

    // set to 20MB
    KafkaCluster spyKafkaCluster = spy(kafkaCluster);
    doReturn(20*1024*1024L).when(spyKafkaCluster).getMaxBytesIn(topicPartition);
    doReturn(0L).when(spyKafkaCluster).getMaxBytesOut(topicPartition);

    Node leader = nodes[0];
    Node[] replicas = new Node[]{
        nodes[0],
        nodes[1],
        nodes[2],
    };

    Node[] isrs = new Node[]{
        nodes[0]
    };
    PartitionInfo partitionInfo = new PartitionInfo(TOPIC, 0, leader, replicas, isrs);
    OutOfSyncReplica oosReplica = new OutOfSyncReplica(partitionInfo);
    oosReplica.replicaBrokers = Arrays.asList(new Integer[]{0,1,2});


    KafkaBroker[] brokers = new KafkaBroker[]{
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 0),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 1),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 2),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 3),
        new KafkaBroker(doctorKafkaClusterConfig, spyKafkaCluster, 4)
    };

    for ( KafkaBroker broker : brokers ){
      BrokerStats bs = new BrokerStats();
      bs.setTimestamp(System.currentTimeMillis());
      broker.setLatestStats(bs);
      kafkaCluster.brokers.put(broker.getId(), broker);
    }

    Set<TopicPartition> replicaSet = new HashSet<>();
    replicaSet.add(topicPartition);

    for ( Node node : replicas ) {
      brokers[node.id()].setFollowerReplicas(replicaSet);
    }

    brokers[leader.id()].setLeaderReplicas(replicaSet);

    // a list of assignments of localities to brokers with the same size as LOCALITIES
    List<List<Integer>> testLocalityAssignments = Arrays.asList(
        Arrays.asList(0),
        Arrays.asList(1, 3),
        Arrays.asList(2, 4)
    );

    for (int localityId = 0; localityId < testLocalityAssignments.size(); localityId++) {
      for(int brokerId : testLocalityAssignments.get(localityId)){
        brokers[brokerId].setRackId(LOCALITIES[localityId]);
      }
    }

    // setting the reservedBytesIn for priority queue comparisons
    brokers[4].reserveBandwidth(TOPIC_PARTITION, 20*1024*1024, 10);

    Map<String, PriorityQueue<KafkaBroker>> brokerLocalityMap =
        kafkaCluster.getBrokerQueueByLocality();
    Map<OutOfSyncReplica, Map <Integer, String>> reassignmentToLocalityFailures = new HashMap<>();

    double inBoundReq = spyKafkaCluster.getMaxBytesIn(oosReplica.topicPartition);
    double outBoundReq = spyKafkaCluster.getMaxBytesOut(oosReplica.topicPartition);
    int preferredBroker = oosReplica.replicaBrokers.get(0);

    // Test getAlternativeBrokersByLocality,
    // brokers 1 & 2 are out of sync
    // assignment fails since not enough bandwidth to migrate from 2 -> 4

    Map<Integer, KafkaBroker> localityReassignments =
        kafkaCluster.getAlternativeBrokersByLocality(
            brokerLocalityMap,
            oosReplica,
            inBoundReq,
            outBoundReq,
            preferredBroker
        );

    verify(spyKafkaCluster,atLeast(2)).getMaxBytesIn(topicPartition);
    verify(spyKafkaCluster,atLeast(2)).getMaxBytesIn(topicPartition);
    assertNull(localityReassignments);
  }

}