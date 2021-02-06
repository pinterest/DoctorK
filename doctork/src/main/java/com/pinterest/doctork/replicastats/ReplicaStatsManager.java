package com.pinterest.doctork.replicastats;

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

import com.pinterest.doctork.BrokerStats;
import com.pinterest.doctork.KafkaCluster;
import com.pinterest.doctork.config.DoctorKConfig;
import com.pinterest.doctork.util.KafkaUtils;
import com.pinterest.doctork.util.ReplicaStatsUtil;

public class ReplicaStatsManager {

  private static final Logger LOG = LogManager.getLogger(ReplicaStatsManager.class);

  private ConcurrentMap<String, KafkaCluster> clusters = new ConcurrentHashMap<>();
  private DoctorKConfig config;

  public ConcurrentMap<String, KafkaCluster> getClusters() {
    return clusters;
  }

  public DoctorKConfig getConfig() {
    return config;
  }

  public Set<String> getClusterZkUrls() {
    return clusterZkUrls;
  }

  private Set<String> clusterZkUrls;

  public ReplicaStatsManager(DoctorKConfig config){
    this.config = config;
    this.clusterZkUrls = config.getClusterZkUrls();
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

    KafkaCluster cluster = clusters.computeIfAbsent(brokerZkUrl, url -> new KafkaCluster(url, config.getClusterConfigByZkUrl(url)));
    cluster.recordBrokerStats(brokerStats);
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