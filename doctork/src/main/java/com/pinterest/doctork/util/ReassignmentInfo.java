package com.pinterest.doctork.util;

import com.pinterest.doctork.KafkaBroker;

import org.apache.kafka.common.TopicPartition;

public class ReassignmentInfo {

    public TopicPartition topicPartition;
    public KafkaBroker source;
    public KafkaBroker dest;

    public ReassignmentInfo(TopicPartition tp, KafkaBroker src, KafkaBroker dest) {
      this.topicPartition = tp;
      this.source = src;
      this.dest = dest;
    }

    @Override
    public String toString() {
      String result = topicPartition.toString() + ": ";
      result += source.getName() + " -> " + dest.getName();
      return result;
    }
}
