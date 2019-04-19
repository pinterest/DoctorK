package com.pinterest.doctorkafka.replicastats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.ReplicaStat;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.ReplicaStatsUtil;

public class ReplicaStatsManager {

  private static final Logger LOG = LogManager.getLogger(ReplicaStatsManager.class);

  private static final int SLIDING_WINDOW_SIZE = 1440 * 4;

  /**
   * The kafka network traffic stats takes ~15 minutes to cool down. We give a 20 minutes
   * cool down period to avoid inaccurate stats collection.
   */
  private static final long REASSIGNMENT_COOLDOWN_WINDOW_IN_MS = 1800 * 1000L;

  private ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>>
      bytesInStats = new ConcurrentHashMap<>();

  private ConcurrentMap<String, ConcurrentMap<TopicPartition, Histogram>>
      bytesOutStats = new ConcurrentHashMap<>();

  private ConcurrentMap<String, KafkaCluster> clusters = new ConcurrentHashMap<>();

  private DoctorKafkaConfig config;

  public ConcurrentHashMap<String, ConcurrentHashMap<TopicPartition, Long>>
      replicaReassignmentTimestamps = new ConcurrentHashMap<>();

  /*
   * Getters
   */

  public ConcurrentMap<String, KafkaCluster> getClusters() {
    return clusters;
  }

  public DoctorKafkaConfig getConfig() {
    return config;
  }

  public Set<String> getClusterZkUrls() {
    return clusterZkUrls;
  }

  private Set<String> clusterZkUrls;

  public ReplicaStatsManager(DoctorKafkaConfig config){
    this.config = config;
    this.clusterZkUrls = config.getClusterZkUrls();
  }

  public void updateReplicaReassignmentTimestamp(String brokerZkUrl,
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

  private long getLastReplicaReassignmentTimestamp(String brokerZkUrl,
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
  public void update(BrokerStats brokerStats) {
    String brokerZkUrl = brokerStats.getZkUrl();
    // ignore the brokerstats from clusters that are not enabled operation automation.
    if (brokerZkUrl == null || !clusterZkUrls.contains(brokerZkUrl)) {
      return;
    }

    KafkaCluster cluster = clusters.computeIfAbsent(brokerZkUrl, url -> new KafkaCluster(url, config.getClusterConfigByZkUrl(url), this));
    cluster.recordBrokerStats(brokerStats);

    bytesInStats.putIfAbsent(brokerZkUrl, new ConcurrentHashMap<>());
    bytesOutStats.putIfAbsent(brokerZkUrl, new ConcurrentHashMap<>());

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

        bytesInHistograms.computeIfAbsent(topicPartition, k -> new Histogram(new SlidingWindowReservoir(SLIDING_WINDOW_SIZE)));
        bytesOutHistograms.computeIfAbsent(topicPartition, k -> new Histogram(new SlidingWindowReservoir(SLIDING_WINDOW_SIZE)));

        bytesInHistograms.get(topicPartition).update(replicaStat.getBytesIn15MinMeanRate());
        bytesOutHistograms.get(topicPartition).update(replicaStat.getBytesOut15MinMeanRate());
      }
    }
  }


  public long getMaxBytesIn(String zkUrl, TopicPartition topicPartition) {
    try {
      return bytesInStats.get(zkUrl).get(topicPartition).getSnapshot().getMax();
    } catch (Exception e) {
      LOG.error("Failed to get bytesinfo for {}:{}", zkUrl, topicPartition);
      throw e;
    }
  }

  public double get99thPercentilBytesIn(String zkUrl, TopicPartition topicPartition) {
    return bytesInStats.get(zkUrl).get(topicPartition).getSnapshot().get99thPercentile();
  }

  public long getMaxBytesOut(String zkUrl, TopicPartition topicPartition) {
    return bytesOutStats.get(zkUrl).get(topicPartition).getSnapshot().getMax();
  }

  public double get99thPercentilBytesOut(String zkUrl, TopicPartition topicPartition) {
    return bytesOutStats.get(zkUrl).get(topicPartition).getSnapshot().get99thPercentile();
  }


  public Map<TopicPartition, Histogram> getTopicsBytesInStats(String zkUrl) {
    return bytesInStats.get(zkUrl);
  }

  public Map<TopicPartition, Histogram> getTopicsBytesOutStats(String zkUrl) {
    return bytesOutStats.get(zkUrl);
  }

  /**
   * Read the replica stats in the past 24 - 48 hours, based on the configuration setting.
   * @param zkUrl
   */
  public void readPastReplicaStats(String zkUrl,
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
    
    offsets = ReplicaStatsUtil
        .getProcessingStartOffsets(kafkaConsumer, brokerStatsTopic, startTimestampInMillis);

    kafkaConsumer.unsubscribe();
    kafkaConsumer.assign(offsets.keySet());
    Map<TopicPartition, Long> latestOffsets = kafkaConsumer.endOffsets(offsets.keySet());
    KafkaUtils.closeConsumer(zkUrl);

    List<PastReplicaStatsProcessor> processors = new ArrayList<>();

    for (TopicPartition tp : latestOffsets.keySet()) {
      PastReplicaStatsProcessor processor;
      processor = new PastReplicaStatsProcessor(zkUrl, securityProtocol, tp, offsets.get(tp), latestOffsets.get(tp), this);
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