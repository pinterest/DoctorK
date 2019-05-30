package com.pinterest.doctorkafka.modules.context.cluster.kafka;

import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.modules.context.cluster.ClusterContext;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import kafka.utils.ZkUtils;

public class KafkaContext extends ClusterContext {
  private KafkaCluster kafkaCluster;
  private ZkUtils zkUtils;

  // a per-Kafka-cluster client for Doctorkafka interactions with Zookeeper. Dependency should be
  // injected externally from container of plugin.
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
}
