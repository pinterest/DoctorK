package com.pinterest.doctorkafka;


import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.util.OutOfSyncReplica;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * KafkaCluster captures the status of one kafka cluster. It has the following information:
 *    1. topic list
 *    2. the replica resource requirement stats of each replica
 *    3. partition assignment status. the current partition assignment
 *
 *  We track the topic partition resource utilization at the cluster level, as the replica
 *  stats at the host level can be affected by various factors, e.g. partition re-assignment,
 *  moving partition to a new broker, changed data retention time, network saturation of other
 *  brokers, etc.
 */
public class KafkaCluster {

  private static final Logger LOG = LogManager.getLogger(KafkaCluster.class);
  private static final int MAX_NUM_STATS = 5;

  private DoctorKafkaClusterConfig clusterConfig;
  public String zkUrl;
  public ConcurrentMap<Integer, KafkaBroker> brokers;
  private ConcurrentMap<Integer, LinkedList<BrokerStats>> brokerStatsMap;
  public ConcurrentMap<String, Set<TopicPartition>> topicPartitions = new ConcurrentHashMap<>();

  public KafkaCluster(String zookeeper, DoctorKafkaClusterConfig clusterConfig) {
    this.zkUrl = zookeeper;
    this.brokers = new ConcurrentHashMap<>();
    this.clusterConfig = clusterConfig;
    this.brokerStatsMap = new ConcurrentHashMap<>();
  }

  public int size() {
    return brokers.size();
  }

  public String name() {
    return clusterConfig.getClusterName();
  }

  /**
   * Update the broker stats. Note that a broker may continue to send brokerStats that contains
   * failure info after the kafka process fails.
   *
   * @param brokerStats  the broker stats
   */
  public void recordBrokerStats(BrokerStats brokerStats) {
    try {
      int brokerId = brokerStats.getId();
      LinkedList<BrokerStats> brokerStatsList = brokerStatsMap.computeIfAbsent(brokerId, i -> new LinkedList<>());

      // multiple PastReplicaStatsProcessor/BrokerStatsProcessor may be processing BrokerStats
      // for the same broker simultaneously, thus enforcing single writes here
      synchronized (brokerStatsList){
        if (brokerStatsList.size() == MAX_NUM_STATS) {
          brokerStatsList.removeFirst();
        }
        brokerStatsList.addLast(brokerStats);
      }

      if (!brokerStats.getHasFailure()) {
        // only record brokerstat when there is no failure on that broker.
        KafkaBroker broker = brokers.computeIfAbsent(brokerId, i -> new KafkaBroker(clusterConfig, i));
        broker.update(brokerStats);
      }

      if (brokerStats.getLeaderReplicas() != null) {
        for (AvroTopicPartition atp : brokerStats.getLeaderReplicas()) {
          String topic = atp.getTopic();
          TopicPartition tp = new TopicPartition(topic, atp.getPartition());
          topicPartitions
              .computeIfAbsent(topic, t -> new HashSet<>())
              .add(tp);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to read broker stats : {}", brokerStats, e);
    }
  }

  public JsonElement toJson() {
    JsonObject json = new JsonObject();
    JsonArray jsonBrokers = new JsonArray();
    json.add("brokers", jsonBrokers);

    List<KafkaBroker> result = new ArrayList<>();

    synchronized (brokers) {
      for (KafkaBroker broker : brokers.values()) {
	  jsonBrokers.add(broker.toJson());
      }
    }
    return json;
  }

  /**
   * Get broker by broker id.
   *
   * @param id  the broker id
   * @return KafkaBroker object for the broker with id @id
   */
  public KafkaBroker getBroker(int id) {
    if (!brokers.containsKey(id)) {
      return null;
    }
    return brokers.get(id);
  }

  /**
   * Get the latest stats for a broker.
   *
   * @param brokerId broker id
   * @return the latest broker stats
   */
  public BrokerStats getLatestBrokerStats(int brokerId) {
    synchronized (brokers) {
      if (!brokers.containsKey(brokerId)) {
        LOG.info("Failed to find broker {} in cluster {}", brokerId, zkUrl);
        return null;
      }
      KafkaBroker broker = brokers.get(brokerId);
      return broker.getLatestStats();
    }
  }

  public List<BrokerStats> getBrokerStatsList(int brokerId) {
    synchronized (brokers) {
      if (!brokerStatsMap.containsKey(brokerId)) {
        LOG.info("Failed to find broker {} in cluster {}", brokerId, zkUrl);
        return null;
      }
      return brokerStatsMap.get(brokerId);
    }
  }

  /**
   *  We consider a broker is of high traffic if either in-bound traffic or
   *  out-bound traffic exceeds the expected mean traffic.
   *
   *  @return the list of kafka brokers that exceeds the network traffic limit.
   */
  public List<KafkaBroker> getHighTrafficBrokers() {
    double averageBytesIn = getMaxBytesIn() / (double) brokers.size();
    double averageBytesOut = getMaxBytesOut() / (double) brokers.size();
    double bytesInLimit = clusterConfig.getNetworkInLimitInBytes();
    double bytesOutLimit = clusterConfig.getNetworkOutLimitInBytes();

    List<KafkaBroker> result = new ArrayList<>();
    synchronized (brokers) {
      for (KafkaBroker broker : brokers.values()) {
        double brokerBytesIn = broker.getMaxBytesIn();
        double brokerBytesOut = broker.getMaxBytesOut();
        if (brokerBytesIn < averageBytesIn && brokerBytesOut < averageBytesOut) {
          continue;
        }
        if (brokerBytesIn < bytesInLimit && brokerBytesOut < bytesOutLimit) {
          continue;
        }
        LOG.debug("High traffic broker: {} : [{}, {}]",
            broker.name(), broker.getMaxBytesIn(), broker.getMaxBytesOut());
        result.add(broker);
      }
    }
    return result;
  }


  public List<KafkaBroker> getLowTrafficBrokers() {
    double averageBytesIn = getMaxBytesIn() / (double) brokers.size();
    double averageBytesOut = getMaxBytesOut() / (double) brokers.size();

    List<KafkaBroker> result = new ArrayList<>();
    synchronized (brokers) {
      for (KafkaBroker broker : brokers.values()) {
        try {
          double brokerBytesIn = broker.getMaxBytesIn();
          double brokerBytesOut = broker.getMaxBytesOut();
          if (brokerBytesIn < averageBytesIn && brokerBytesOut < averageBytesOut) {
            LOG.info("Low traffic broker {} : [{}, {}]",
                broker.name(), broker.getMaxBytesIn(), broker.getMaxBytesOut());
            result.add(broker);
          }
        } catch (Exception e) {
          LOG.info("catch unexpected exception");
        }
      }
    }
    return result;
  }

  public PriorityQueue<KafkaBroker> getBrokerQueue() {
    PriorityQueue<KafkaBroker> brokerQueue =
        new PriorityQueue<>(new KafkaBroker.KafkaBrokerComparator());
    for (Map.Entry<Integer, KafkaBroker> entry : brokers.entrySet()) {
      KafkaBroker broker = entry.getValue();
      BrokerStats latestStats = broker.getLatestStats();
      if (latestStats == null
          || latestStats.getHasFailure()
          || System.currentTimeMillis() - latestStats.getTimestamp() > 240000) {
        continue;
      }
      brokerQueue.add(broker);
    }
    return brokerQueue;
  }


  /**
   * Get the broker Id that has the resource. Here we need to apply the proper placement policy.
   *
   * @param brokerQueue  the list of brokers that are sorted in resource usage
   * @param oosReplica  out of sync replicas
   * @return a BrokerId to KafkaBroker mapping
   */
  public Map<Integer, KafkaBroker> getAlternativeBrokers(PriorityQueue<KafkaBroker> brokerQueue,
                                                          OutOfSyncReplica oosReplica) {
    TopicPartition topicPartition = oosReplica.topicPartition;
    double inBoundReq = ReplicaStatsManager.getMaxBytesIn(zkUrl, topicPartition);
    double outBoundReq = ReplicaStatsManager.getMaxBytesOut(zkUrl, topicPartition);
    int preferredBroker = oosReplica.replicaBrokers.get(0);

    boolean success = true;
    Map<Integer, KafkaBroker> result = new HashMap<>();
    List<KafkaBroker> unusableBrokers = new ArrayList<>();
    for (int oosBrokerId : oosReplica.outOfSyncBrokers) {
      // we will get the broker with the least network usage
      KafkaBroker leastUsedBroker = brokerQueue.poll();
      while (leastUsedBroker != null && oosReplica.replicaBrokers.contains(leastUsedBroker.id())) {
        unusableBrokers.add(leastUsedBroker);
        leastUsedBroker = brokerQueue.poll();
      }
      if (leastUsedBroker == null) {
        LOG.error("Failed to find a usable broker for fixing {}:{}", oosReplica, oosBrokerId);
        success = false;
      } else {
        LOG.info("LeastUsedBroker for replacing {} : {}", oosBrokerId, leastUsedBroker.id());
        success &= leastUsedBroker.reserveInBoundBandwidth(topicPartition, inBoundReq);
        if (preferredBroker == oosBrokerId) {
          success &= leastUsedBroker.reserveOutBoundBandwidth(topicPartition, outBoundReq);
        }
        if (success) {
          result.put(oosBrokerId, leastUsedBroker);
          // the broker should not be used again for this topic partition.
          unusableBrokers.add(leastUsedBroker);
        } else {
          LOG.error("Failed to allocate resource to replace {}:{}", oosReplica, oosBrokerId);
          success = false;
        }
      }
    }
    // push the brokers back to brokerQueue to keep invariant true
    brokerQueue.addAll(unusableBrokers);
    return success ? result : null;
  }


  public KafkaBroker getAlternativeBroker(TopicPartition topicPartition,
                                          double tpBytesIn, double tpBytesOut) {
    PriorityQueue<KafkaBroker> brokerQueue =
        new PriorityQueue<>(new KafkaBroker.KafkaBrokerComparator());

    for (Map.Entry<Integer, KafkaBroker> entry : brokers.entrySet()) {
      KafkaBroker broker = entry.getValue();
      if (!broker.hasTopicPartition(topicPartition)) {
        brokerQueue.add(broker);
      }
    }
    // we will get the broker with the least network usage
    KafkaBroker leastUsedBroker = brokerQueue.poll();
    LOG.info("LeastUsedBroker for replacing {} : {}", topicPartition, leastUsedBroker.id());
    boolean success = leastUsedBroker.reserveInBoundBandwidth(topicPartition, tpBytesIn);
    success &= leastUsedBroker.reserveOutBoundBandwidth(topicPartition, tpBytesOut);

    if (!success) {
      LOG.error("Failed to allocate resource to replace {}", topicPartition);
      return null;
    } else {
      return leastUsedBroker;
    }
  }


  public long getMaxBytesIn() {
    long result = 0L;
    for (Map.Entry<String, Set<TopicPartition>> entry : topicPartitions.entrySet()) {
      Set<TopicPartition> topicPartitions = entry.getValue();
      for (TopicPartition tp : topicPartitions) {
        result += ReplicaStatsManager.getMaxBytesIn(zkUrl, tp);
      }
    }
    return result;
  }


  public long getMaxBytesOut() {
    long result = 0L;
    for (Map.Entry<String, Set<TopicPartition>> entry : topicPartitions.entrySet()) {
      Set<TopicPartition> topicPartitions = entry.getValue();
      for (TopicPartition tp : topicPartitions) {
        result += ReplicaStatsManager.getMaxBytesOut(zkUrl, tp);
      }
    }
    return result;
  }


  /**
   *  Clear the network allocation related data once parttion reassignment is done
   */
  public void clearResourceAllocationCounters() {
    for (KafkaBroker broker : brokers.values()) {
      broker.clearResourceAllocationCounters();
    }
  }

  @Override
  public String toString() {
    StringBuilder strBuilder = new StringBuilder();
    TreeMap<Integer, KafkaBroker> treeMap = new TreeMap<>(brokers);
    for (Map.Entry<Integer, KafkaBroker> entry : treeMap.entrySet()) {
      strBuilder.append("   " + entry.getKey() + " : ");
      strBuilder.append(entry.getValue() + "\n");
    }
    return strBuilder.toString();
  }
}
