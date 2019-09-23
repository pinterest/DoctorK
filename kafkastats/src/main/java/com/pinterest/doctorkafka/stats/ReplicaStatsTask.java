package com.pinterest.doctorkafka.stats;


import com.pinterest.doctorkafka.ReplicaStat;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;


/**
 *  Log size metric on each topic partition.
 *
 *  log size:  kafka.log:type=Log,name=Size,topic=%s,partition=%d"*
 *  num log segs: kafka.log:type=Log,name=NumLogSegments,topic=%s,partition=%d
 *  start offset: kafka.log:type=Log,name=LogStartOffset,topic=%s,partition=%d
 *  end offset:   kafka.log:type=Log,name=LogEndOffset,topic=%s,partition=%d
 *  under-rep:    kafka.cluster:type=Partition,name=UnderReplicated,topic=%s,partition=%d
 *
 */
public class ReplicaStatsTask implements Callable<ReplicaStat> {

  private static final Logger LOG = LogManager.getLogger(ReplicaStatsTask.class);

  private MBeanServerConnection mbs;
  private TopicPartition topicPartition;
  private boolean isLeader;
  private boolean inReassignment;

  public ReplicaStatsTask(MBeanServerConnection mbs, TopicPartition topicPartition,
                          boolean isLeader, boolean inReassignment) {
    this.mbs = mbs;
    this.topicPartition = topicPartition;
    this.isLeader = isLeader;
    this.inReassignment = inReassignment;
  }

  @Override
  public ReplicaStat call() throws Exception {

    ReplicaStat replicaStat = new ReplicaStat();
    replicaStat.setTimestamp(System.currentTimeMillis());
    replicaStat.setTopic(topicPartition.topic());
    replicaStat.setPartition(topicPartition.partition());

    String logSizeMetric = String.format("kafka.log:type=Log,name=Size,topic=%s,partition=%d",
        topicPartition.topic(), topicPartition.partition());

    LOG.info("logSizeMetric = {}", logSizeMetric);

    Long longValue = 0L;
    try {
      longValue = (Long) mbs.getAttribute(new ObjectName(logSizeMetric), "Value");
      replicaStat.setLogSizeInBytes(longValue);
    } catch (InstanceNotFoundException e) {
      LOG.info("Could not find metric {}", logSizeMetric, e);
    }

    String numSegmentsMetric = String.format(
        "kafka.log:type=Log,name=NumLogSegments,topic=%s,partition=%d",
        topicPartition.topic(), topicPartition.partition());
    int intValue;
    try {
      intValue = (Integer) mbs.getAttribute(new ObjectName(numSegmentsMetric), "Value");
      replicaStat.setNumLogSegments(intValue);
    } catch (InstanceNotFoundException e) {
      LOG.info("Could not find metric {}", numSegmentsMetric, e);
    }

    String startOffsetMetric = String.format(
        "kafka.log:type=Log,name=LogStartOffset,topic=%s,partition=%d",
        topicPartition.topic(), topicPartition.partition());
    try {
      longValue = (Long) mbs.getAttribute(new ObjectName(startOffsetMetric), "Value");
      replicaStat.setStartOffset(longValue);
    } catch (InstanceNotFoundException e) {
      LOG.info("Could not find metric {}", startOffsetMetric, e);
    }

    String endOffsetMetric = String.format(
        "kafka.log:type=Log,name=LogEndOffset,topic=%s,partition=%d",
        topicPartition.topic(), topicPartition.partition());
    try {
      longValue = (Long) mbs.getAttribute(new ObjectName(endOffsetMetric), "Value");
      replicaStat.setEndOffset(longValue);
    } catch (InstanceNotFoundException e) {
      LOG.info("Could not find metric {}", endOffsetMetric, e);
    }

    String underReplicatedMetric = String.format(
        "kafka.cluster:type=Partition,name=UnderReplicated,topic=%s,partition=%d",
        topicPartition.topic(), topicPartition.partition());
    try {
      intValue = (Integer) mbs.getAttribute(new ObjectName(underReplicatedMetric), "Value");
      replicaStat.setUnderReplicated(intValue != 0);
    } catch (InstanceNotFoundException e) {
      LOG.info("Could not find metric {}", underReplicatedMetric, e);
    }
    replicaStat.setIsLeader(isLeader);
    replicaStat.setInReassignment(inReassignment);
    return replicaStat;
  }
}
