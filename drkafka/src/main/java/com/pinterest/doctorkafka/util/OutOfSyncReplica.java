package com.pinterest.doctorkafka.util;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


/**
 *  This class captures info for out-of-sync replicas.
 *
 *     Topic: topic_name    Partition: 33    Leader: 9	 Replicas: 12,9,1    Isr: 12,9
 *     Topic: topic_name    Partition: 34    Leader: 10	 Replicas: 1,2,10    Isr: 2,10
 *
 */
public class OutOfSyncReplica {

  private int hash = 0;

  public TopicPartition topicPartition;

  public Node leader;

  public List<Integer> replicaBrokers;

  public Set<Integer> inSyncBrokers;

  public Set<Integer> outOfSyncBrokers;

  public OutOfSyncReplica(PartitionInfo partitionInfo) {
    this.topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    this.inSyncBrokers = getInSyncReplicas(partitionInfo);
    this.outOfSyncBrokers = getOutOfSyncReplicas(partitionInfo);
    this.leader = partitionInfo.leader();
  }

  public String topic() {
    return topicPartition.topic();
  }

  public int partition() {
    return topicPartition.partition();
  }


  public Set<Integer>  getInSyncReplicas(PartitionInfo  partitionInfo) {
    Set<Integer> result = new HashSet<>();
    for (Node node: partitionInfo.inSyncReplicas()) {
      result.add(node.id());
    }
    return result;
  }

  /**
   * Get the under replicated nodes from PartitionInfo
   */
  public static Set<Integer> getOutOfSyncReplicas(PartitionInfo partitionInfo) {
    if (partitionInfo.inSyncReplicas().length == partitionInfo.replicas().length) {
      return new HashSet<>();
    }
    Set<Node> nodes = new HashSet(Arrays.asList(partitionInfo.replicas()));
    for (Node node : partitionInfo.inSyncReplicas()) {
      nodes.remove(node);
    }
    return nodes.stream().map(nd -> nd.id()).collect(Collectors.toSet());
  }


  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }
    final int prime = 31;
    int result = 1;
    result = prime * result + topicPartition.hashCode();

    if (replicaBrokers != null) {
      for (int brokerId: replicaBrokers) {
        result = prime * result + brokerId;
      }
    }

    if (inSyncBrokers != null) {
      for (int brokerId : inSyncBrokers) {
        result = prime * result + brokerId;
      }
    }

    if (outOfSyncBrokers != null) {
      for (int brokerId : outOfSyncBrokers) {
        result = prime * result + brokerId;
      }
    }
    this.hash = result;
    return result;
  }

  @Override
  public String toString() {
    return topicPartition.toString();
  }
}
