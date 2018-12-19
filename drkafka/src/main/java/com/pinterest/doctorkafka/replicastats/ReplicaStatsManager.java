package com.pinterest.doctorkafka.replicastats;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.ReplicaStat;
import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.OperatorUtil;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ReplicaStatsManager {

  private static final Logger LOG = LogManager.getLogger(ReplicaStatsManager.class);

  private static final int SLIDING_WINDOW_SIZE = 1440 * 4;

  /**
   * The kafka network traffic stats takes ~15 minutes to cool down. We give a 20 minutes
   * cool down period to avoid inaccurate stats collection.
   */
  private static final long REASSIGNMENT_COOLDOWN_WINDOW_IN_MS = 1800 * 1000L;

  public static ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>>
      bytesInStats = new ConcurrentHashMap<>();

  public static ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>>
      bytesOutStats = new ConcurrentHashMap<>();

  public static ConcurrentMap<String, KafkaCluster> clusters = new ConcurrentHashMap<>();

  public static DoctorKafkaConfig config = null;

  public static ConcurrentHashMap<String, ConcurrentHashMap<TopicPartition, Long>>
      replicaReassignmentTimestamps = new ConcurrentHashMap<>();

  public static void initialize(DoctorKafkaConfig conf) {
    config = conf;
  }

  public static void updateReplicaReassignmentTimestamp(String brokerZkUrl,
                                                         ReplicaStat replicaStat) {
    if (!replicaReassignmentTimestamps.containsKey(brokerZkUrl)) {
      replicaReassignmentTimestamps.put(brokerZkUrl, new ConcurrentHashMap<>());
    }
    ConcurrentHashMap<TopicPartition, Long> replicaTimestamps =
        replicaReassignmentTimestamps.get(brokerZkUrl);
    TopicPartition topicPartition = new TopicPartition(
        replicaStat.getTopic(), replicaStat.getPartition());

    if (!replicaTimestamps.containsKey(topicPartition) ||
        replicaTimestamps.get(topicPartition) < replicaStat.getTimestamp()) {
      replicaTimestamps.put(topicPartition, replicaStat.getTimestamp());
    }
  }

  private static long getLastReplicaReassignmentTimestamp(String brokerZkUrl,
                                                          TopicPartition topicPartition) {
    long result = 0;
    if (replicaReassignmentTimestamps.containsKey(brokerZkUrl)) {
      ConcurrentHashMap<TopicPartition, Long> replicaTimestamps =
          replicaReassignmentTimestamps.get(brokerZkUrl);
      if (replicaTimestamps.containsKey(topicPartition)) {
        result = replicaTimestamps.get(topicPartition);
      }
    }
    return result;
  }


  /**
   *  Record the latest brokerstats, and update DocotorKafka internal data structures.
   */
  public static void update(BrokerStats brokerStats) {
    String brokerZkUrl = brokerStats.getZkUrl();
    // ignore the brokerstats from clusters that are not enabled operation automation.
    if (!config.getClusterZkUrls().contains(brokerZkUrl)) {
      return;
    }

    KafkaCluster cluster;
    synchronized (clusters) {
      if (brokerZkUrl != null && !clusters.containsKey(brokerZkUrl)) {
        DoctorKafkaClusterConfig clusterConfig = config.getClusterConfigByZkUrl(brokerZkUrl);
        assert clusterConfig != null;

        cluster = new KafkaCluster(brokerZkUrl, clusterConfig);
        clusters.put(brokerZkUrl, cluster);
      }
      cluster = clusters.get(brokerZkUrl);
    }
    cluster.recordBrokerStats(brokerStats);

    if (!bytesInStats.containsKey(brokerZkUrl)) {
      bytesInStats.put(brokerZkUrl, new ConcurrentHashMap<>());
    }
    if (!bytesOutStats.containsKey(brokerZkUrl)) {
      bytesOutStats.put(brokerZkUrl, new ConcurrentHashMap<>());
    }

    if (brokerStats.getLeaderReplicaStats() != null) {
      ConcurrentMap<TopicPartition, Histogram> bytesInHistograms = bytesInStats.get(brokerZkUrl);
      ConcurrentMap<TopicPartition, Histogram> bytesOutHistograms = bytesOutStats.get(brokerZkUrl);
      for (ReplicaStat replicaStat : brokerStats.getLeaderReplicaStats()) {
        if (replicaStat.getInReassignment()) {
          // if the replica is involved in reassignment, ignore the stats
          updateReplicaReassignmentTimestamp(brokerZkUrl, replicaStat);
          continue;
        }
        TopicPartition topicPartition = new TopicPartition(
            replicaStat.getTopic(), replicaStat.getPartition());
        long lastReassignment = getLastReplicaReassignmentTimestamp(brokerZkUrl, topicPartition);
        if (brokerStats.getTimestamp() - lastReassignment < REASSIGNMENT_COOLDOWN_WINDOW_IN_MS) {
          continue;
        }

        if (!bytesInHistograms.containsKey(topicPartition)) {
          Histogram histogram = new Histogram(new SlidingWindowReservoir(SLIDING_WINDOW_SIZE));
          bytesInHistograms.putIfAbsent(topicPartition, histogram);
        }

        if (!bytesOutHistograms.containsKey(topicPartition)) {
          Histogram histogram = new Histogram(new SlidingWindowReservoir(SLIDING_WINDOW_SIZE));
          bytesOutHistograms.putIfAbsent(topicPartition, histogram);
        }

        bytesInHistograms.get(topicPartition).update(replicaStat.getBytesIn15MinMeanRate());
        bytesOutHistograms.get(topicPartition).update(replicaStat.getBytesOut15MinMeanRate());
      }
    }
  }


  public static long getMaxBytesIn(String zkUrl, TopicPartition topicPartition) {
    try {
      return bytesInStats.get(zkUrl).get(topicPartition).getSnapshot().getMax();
    } catch (Exception e) {
      LOG.error("Failed to get bytesinfo for {}:{}", zkUrl, topicPartition);
      throw e;
    }
  }

  public static double get99thPercentilBytesIn(String zkUrl, TopicPartition topicPartition) {
    return bytesInStats.get(zkUrl).get(topicPartition).getSnapshot().get99thPercentile();
  }

  public static long getMaxBytesOut(String zkUrl, TopicPartition topicPartition) {
    return bytesOutStats.get(zkUrl).get(topicPartition).getSnapshot().getMax();
  }

  public static double get99thPercentilBytesOut(String zkUrl, TopicPartition topicPartition) {
    return bytesOutStats.get(zkUrl).get(topicPartition).getSnapshot().get99thPercentile();
  }


  public static Map<TopicPartition, Histogram> getTopicsBytesInStats(String zkUrl) {
    return bytesInStats.get(zkUrl);
  }

  public static Map<TopicPartition, Histogram> getTopicsBytesOutStats(String zkUrl) {
    return bytesOutStats.get(zkUrl);
  }
  
  public static Map<TopicPartition, Long> getProcessingStartOffsetsNew(KafkaConsumer<?, ?> kafkaConsumer,
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
    for (Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
      partitionMap.put(entry.getKey(), entry.getValue().offset());
    }
    return partitionMap;
  }

  /**
   * Find the start offsets for the processing windows. We uses kafka 0.10.1.1 that does not support
   * KafkaConsumer.
   */
  public static Map<TopicPartition, Long> getProcessingStartOffsets(KafkaConsumer kafkaConsumer,
                                                                    String brokerStatsTopic,
                                                                    long startTimestampInMillis) {
    List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(brokerStatsTopic);
    LOG.info("Get partition info for {} : {} partitions", brokerStatsTopic, partitionInfos.size());
    List<TopicPartition> topicPartitions = partitionInfos.stream()
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList());

    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);
    Map<TopicPartition, Long> beginningOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
    Map<TopicPartition, Long> offsets = new HashMap<>();

    for (TopicPartition tp : topicPartitions) {
      kafkaConsumer.unsubscribe();
      LOG.info("assigning {} to kafkaconsumer", tp);
      List<TopicPartition> tps = new ArrayList<>();
      tps.add(tp);

      kafkaConsumer.assign(tps);
      long endOffset = endOffsets.get(tp);
      long beginningOffset = beginningOffsets.get(tp);
      long offset = Math.max(endOffsets.get(tp) - 10, beginningOffset);
      ConsumerRecord<byte[], byte[]> record = retrieveOneMessage(kafkaConsumer, tp, offset);
      BrokerStats brokerStats = OperatorUtil.deserializeBrokerStats(record);
      if (brokerStats != null) {
        long timestamp = brokerStats.getTimestamp();
        while (timestamp > startTimestampInMillis) {
          offset = Math.max(beginningOffset, offset - 5000);
          record = retrieveOneMessage(kafkaConsumer, tp, offset);
          brokerStats = OperatorUtil.deserializeBrokerStats(record);
          if (brokerStats == null) {
            break;
          }
          timestamp = brokerStats.getTimestamp();
        }
      }
      offsets.put(tp, offset);
      LOG.info("{}: offset = {}, endOffset = {}, # of to-be-processed messages = {}",
          tp, offset, endOffset, endOffset - offset);
    }
    return offsets;
  }


  private static ConsumerRecord<byte[], byte[]> retrieveOneMessage(KafkaConsumer kafkaConsumer,
                                                                   TopicPartition topicPartition,
                                                                   long offset) {
    kafkaConsumer.seek(topicPartition, offset);
    ConsumerRecords<byte[], byte[]> records;
    ConsumerRecord<byte[], byte[]> record = null;
    while (record == null) {
      records = kafkaConsumer.poll(100);
      if (!records.isEmpty()) {
        LOG.debug("records.count() = {}", records.count());
        List<ConsumerRecord<byte[], byte[]>> reclist = records.records(topicPartition);
        if (reclist != null && !reclist.isEmpty()) {
          record = reclist.get(0);
          break;
        } else {
          LOG.info("recList is null or empty");
        }
      }
    }
    return record;
  }


  /**
   * Read the replica stats in the past 24 - 48 hours, based on the configuration setting.
   * @param zkUrl
   */
  public static void readPastReplicaStats(String zkUrl,
                                          SecurityProtocol securityProtocol,
                                          String brokerStatsTopic,
                                          long backtrackWindowInSeconds) {
    long startTime = System.currentTimeMillis();

    KafkaConsumer<?, ?> kafkaConsumer = KafkaUtils.getKafkaConsumer(zkUrl,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        1, securityProtocol, null);

    long startTimestampInMillis = System.currentTimeMillis() - backtrackWindowInSeconds * 1000L;
    Map<TopicPartition, Long> offsets = null;
    
    offsets = ReplicaStatsManager.getProcessingStartOffsetsNew(
          kafkaConsumer, brokerStatsTopic, startTimestampInMillis);

    kafkaConsumer.unsubscribe();
    kafkaConsumer.assign(offsets.keySet());
    Map<TopicPartition, Long> latestOffsets = kafkaConsumer.endOffsets(offsets.keySet());
    KafkaUtils.closeConsumer(zkUrl);

    List<PastReplicaStatsProcessor> processors = new ArrayList<>();

    for (TopicPartition tp : latestOffsets.keySet()) {
      PastReplicaStatsProcessor processor;
      processor = new PastReplicaStatsProcessor(zkUrl, securityProtocol, tp, offsets.get(tp), latestOffsets.get(tp));
      processors.add(processor);
      processor.start();
    }

    for (PastReplicaStatsProcessor processor : processors) {
      try {
        processor.join();
      } catch (InterruptedException e) {
        LOG.error("ReplicaStatsProcessor is interrupted.", e);
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.info("ReplicaStats bootstrap time : {}", (endTime - startTime) / 1000.0);
  }
}
