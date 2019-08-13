package com.pinterest.doctorkafka.plugins.context.state.cluster.kafka;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.plugins.context.state.cluster.ClusterState;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

public class KafkaState extends ClusterState {

  private List<PartitionInfo> underReplicatedPartitions;
  private List<Broker> noBrokerstatsBrokers;
  private List<KafkaBroker> toBeReplacedBrokers;
  private KafkaCluster kafkaCluster;
  private ZkUtils zkUtils;

  /**
   * a per-Kafka-cluster client for Doctorkafka interactions with Zookeeper.
   */
  private ZookeeperClient kafkaClusterZookeeperClient;
  private String zkUrl;

  public KafkaCluster getKafkaCluster() {
    return kafkaCluster;
  }

  public void setKafkaCluster(KafkaCluster kafkaCluster) {
    this.kafkaCluster = kafkaCluster;
  }

  public ZkUtils getZkUtils() {
    return zkUtils;
  }

  public void setZkUtils(ZkUtils zkUtils) {
    this.zkUtils = zkUtils;
  }

  public ZookeeperClient getKafkaClusterZookeeperClient() {
    return kafkaClusterZookeeperClient;
  }

  public void setKafkaClusterZookeeperClient(ZookeeperClient kafkaClusterZookeeperClient) {
    this.kafkaClusterZookeeperClient = kafkaClusterZookeeperClient;
  }

  public String getZkUrl() {
    return zkUrl;
  }

  public void setZkUrl(String zkUrl) {
    this.zkUrl = zkUrl;
  }

  public List<PartitionInfo> getUnderReplicatedPartitions() {
    return underReplicatedPartitions;
  }

  public void setUnderReplicatedPartitions(
      List<PartitionInfo> underReplicatedPartitions) {
    this.underReplicatedPartitions = underReplicatedPartitions;
  }

  public List<Broker> getNoBrokerstatsBrokers() {
    return noBrokerstatsBrokers;
  }

  public void setNoBrokerstatsBrokers(List<Broker> noBrokerstatsBrokers) {
    this.noBrokerstatsBrokers = noBrokerstatsBrokers;
  }

  public List<KafkaBroker> getToBeReplacedBrokers() {
    return toBeReplacedBrokers;
  }

  public void setToBeReplacedBrokers(List<KafkaBroker> toBeReplacedBrokers) {
    this.toBeReplacedBrokers = toBeReplacedBrokers;
  }
}
