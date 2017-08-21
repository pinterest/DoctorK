package com.pinterest.doctorkafka.util;

import org.apache.kafka.common.TopicPartition;

public class PreferredReplicaElectionInfo {

  public TopicPartition topicPartition;

  public int preferredLeaderBroker;

  public PreferredReplicaElectionInfo(TopicPartition tp, int brokerId) {
    topicPartition = tp;
    preferredLeaderBroker = brokerId;
  }

  @Override
  public String toString() {
    String result = topicPartition.toString() + ": " + preferredLeaderBroker;
    return result;
  }
}
