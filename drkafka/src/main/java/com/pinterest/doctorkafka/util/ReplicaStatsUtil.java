package com.pinterest.doctorkafka.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReplicaStatsUtil {

  public static Map<TopicPartition, Long> getProcessingStartOffsets(
      KafkaConsumer<?, ?> kafkaConsumer,
      String brokerStatsTopic,
      long startTimestampInMillis) {
    List<TopicPartition> tpList = kafkaConsumer.partitionsFor(brokerStatsTopic).stream()
                  .map(p->new TopicPartition(p.topic(), p.partition())).collect(Collectors.toList());
    Map<TopicPartition, Long> partitionMap = new HashMap<>();
    for (TopicPartition topicPartition : tpList) {
      partitionMap.put(topicPartition, startTimestampInMillis);
    }

    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = kafkaConsumer
        .offsetsForTimes(partitionMap);
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
      partitionMap.put(entry.getKey(), entry.getValue().offset());
    }
    return partitionMap;
  }
}
