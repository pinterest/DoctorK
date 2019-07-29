package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.util.OutOfSyncReplica;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
  private static final int INVALID_BROKERSTATS_TIME = 240000;
  /**
   * The kafka network traffic stats takes ~15 minutes to cool down. We give a 20 minutes
   * cool down period to avoid inaccurate stats collection.
   */
  private static final long REASSIGNMENT_COOLDOWN_WINDOW_IN_MS = 1800 * 1000L;
  private static final int SLIDING_WINDOW_SIZE = 1440 * 4;

  public String zkUrl;
  private double bytesInPerSecLimit;
  private double bytesOutPerSecLimit;

  public ConcurrentMap<Integer, KafkaBroker> brokers = new ConcurrentHashMap<>();
  private ConcurrentMap<Integer, LinkedList<BrokerStats>> brokerStatsMap = new ConcurrentHashMap<>();
  public ConcurrentMap<String, Set<TopicPartition>> topicPartitions = new ConcurrentHashMap<>();
  private ConcurrentMap<TopicPartition, Histogram> bytesInHistograms = new ConcurrentHashMap<>();
  private ConcurrentMap<TopicPartition, Histogram> bytesOutHistograms = new ConcurrentHashMap<>();
  private ConcurrentMap<TopicPartition, Long> reassignmentTimestamps = new ConcurrentHashMap<>();

  public KafkaCluster(String zkUrl) {
    this.zkUrl = zkUrl;
  }

  public void setBytesInPerSecLimit(double bytesInPerSecLimit) {
    this.bytesInPerSecLimit = bytesInPerSecLimit;
  }

  public void setBytesOutPerSecLimit(double bytesOutPerSecLimit) {
    this.bytesOutPerSecLimit = bytesOutPerSecLimit;
  }

  public int size() {
    return brokers.size();
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
        KafkaBroker broker = brokers.computeIfAbsent(brokerId, i -> new KafkaBroker(
            zkUrl, this, i, bytesInPerSecLimit, bytesOutPerSecLimit
        ));
        broker.update(brokerStats);
      }

      if (brokerStats.getLeaderReplicaStats() != null) {
        for (ReplicaStat replicaStat : brokerStats.getLeaderReplicaStats()) {
          String topic = replicaStat.getTopic();
          TopicPartition topicPartition = new TopicPartition(topic, replicaStat.getPartition());
          topicPartitions.computeIfAbsent(topic, t -> new HashSet<>()).add(topicPartition);
          // if the replica is involved in reassignment, ignore the stats
          if (replicaStat.getInReassignment()){
            reassignmentTimestamps.compute(topicPartition,
                  (t, v) -> v == null || v < replicaStat.getTimestamp() ? replicaStat.getTimestamp() : v);
            continue;
          }
          long lastReassignment = reassignmentTimestamps.getOrDefault(topicPartition, 0L);
          if (brokerStats.getTimestamp() - lastReassignment < REASSIGNMENT_COOLDOWN_WINDOW_IN_MS) {
            continue;
          }
          bytesInHistograms.computeIfAbsent(topicPartition, k -> new Histogram(new SlidingWindowReservoir(SLIDING_WINDOW_SIZE)));
          bytesOutHistograms.computeIfAbsent(topicPartition, k -> new Histogram(new SlidingWindowReservoir(SLIDING_WINDOW_SIZE)));

          bytesInHistograms.get(topicPartition).update(replicaStat.getBytesIn15MinMeanRate());
          bytesOutHistograms.get(topicPartition).update(replicaStat.getBytesOut15MinMeanRate());
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

  public ConcurrentMap<TopicPartition, Histogram> getBytesInHistograms() {
    return bytesInHistograms;
  }

  public ConcurrentMap<TopicPartition, Histogram> getBytesOutHistograms() {
    return bytesOutHistograms;
  }

  public ConcurrentMap<TopicPartition, Long> getReassignmentTimestamps() {
    return reassignmentTimestamps;
  }

  /**
   * Get broker by broker id.
   *
   * @param id  the broker id
   * @return KafkaBroker object for the broker with id @id
   */
  public KafkaBroker getBroker(int id) {
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

    List<KafkaBroker> result = new ArrayList<>();
    synchronized (brokers) {
      for (KafkaBroker broker : brokers.values()) {
        double brokerBytesIn = broker.getMaxBytesIn();
        double brokerBytesOut = broker.getMaxBytesOut();
        if (brokerBytesIn < averageBytesIn && brokerBytesOut < averageBytesOut) {
          continue;
        }
        if (brokerBytesIn < bytesInPerSecLimit && brokerBytesOut < bytesOutPerSecLimit) {
          continue;
        }
        LOG.debug("High traffic broker: {} : [{}, {}]",
            broker.getName(), broker.getMaxBytesIn(), broker.getMaxBytesOut());
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
                broker.getName(), broker.getMaxBytesIn(), broker.getMaxBytesOut());
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
      if (isInvalidBroker(broker)) {
        continue;
      }
      brokerQueue.add(broker);
    }
    return brokerQueue;
  }

  /**
   *
   * @return a priority queue of brokers for each locality in the cluster ordered by network stats
   */
  public Map<String, PriorityQueue<KafkaBroker>> getBrokerQueueByLocality(){
    Map<String, PriorityQueue<KafkaBroker>> brokerLocalityMap = new HashMap<>();
    Comparator<KafkaBroker> comparator = new KafkaBroker.KafkaBrokerComparator();
    for ( Map.Entry<Integer, KafkaBroker> entry : brokers.entrySet() ){
      KafkaBroker broker = entry.getValue();
      if (isInvalidBroker(broker)){
        continue;
      }
      // add broker to locality queue
      // init queue if queue not present in brokerMap for a locality
      brokerLocalityMap
          .computeIfAbsent(broker.getRackId(), i -> new PriorityQueue<>(comparator))
          .add(broker);
    }
    return brokerLocalityMap;
  }

  /**
   * checks if the broker is invalid for assigning replicas
   * @param broker the broker that we want to check
   * @return true if the broker is invalid for assigning replicas, false if it is valid
   */
  protected boolean isInvalidBroker(KafkaBroker broker) {
    BrokerStats latestStats = broker.getLatestStats();
    return latestStats== null ||
        latestStats.getHasFailure() ||
        System.currentTimeMillis() - latestStats.getTimestamp() > INVALID_BROKERSTATS_TIME ||
        broker.isDecommissioned();
  }


  /**
   * Get the broker Id that has the resource. Here we need to apply the proper placement policy.
   *
   * @param brokerQueue  the list of brokers that are sorted in resource usage
   * @param oosReplica  out of sync replicas
   * @param inBoundReq  inbound traffic
   * @param outBoundReq outbound traffc
   * @param preferredBroker preferred broker id
   * @return a BrokerId to KafkaBroker mapping
   */
  public Map<Integer, KafkaBroker> getAlternativeBrokers(
      PriorityQueue<KafkaBroker> brokerQueue,
      OutOfSyncReplica oosReplica,
      double inBoundReq,
      double outBoundReq,
      int preferredBroker
  ) {

    boolean success = true;
    Map<Integer, KafkaBroker> result = new HashMap<>();
    Set<KafkaBroker> unusableBrokers = new HashSet<>();

    for (int oosBrokerId : oosReplica.outOfSyncBrokers) {
      // we will get the broker with the least network usage
      success = findNextBrokerForOosReplica(
          brokerQueue,
          unusableBrokers,
          oosReplica.replicaBrokers,
          result,
          oosBrokerId,
          oosReplica.topicPartition,
          inBoundReq,
          outBoundReq,
          preferredBroker
      );

      // short circuit if failed to find available broker
      if (!success) {
        break;
      }
    }
    // push the brokers back to brokerQueue to keep invariant true
    brokerQueue.addAll(unusableBrokers);
    return success ? result : null;
  }

  /**
   * Similar to getAlternativeBrokers, but locality aware
   * @param brokerQueueByLocality a map keeping a priority queue of brokers for each locality
   * @param oosReplica out of sync replicas
   * @param inBoundReq  inbound traffic
   * @param outBoundReq outbound traffc
   * @param preferredBroker preferred broker id
   * @return a BrokerId to KafkaBroker mapping
   */
  public Map<Integer, KafkaBroker> getAlternativeBrokersByLocality(
      Map<String, PriorityQueue<KafkaBroker>> brokerQueueByLocality,
      OutOfSyncReplica oosReplica,
      double inBoundReq,
      double outBoundReq,
      int preferredBroker
      ) {

    Map<String, List<Integer>> oosBrokerIdsByLocality = new HashMap<>();
    for ( int oosBrokerId : oosReplica.outOfSyncBrokers) {
      String brokerLocality = brokers.get(oosBrokerId).getRackId();
      oosBrokerIdsByLocality
          .computeIfAbsent(brokerLocality, l -> new ArrayList<>())
          .add(oosBrokerId);
    }

    Map<Integer, KafkaBroker> result = new HashMap<>();
    Map<String, Set<KafkaBroker>> unusableBrokersByLocality = new HashMap<>();

    boolean success = true;
    // Affinity
    for ( Map.Entry<String, List<Integer>> oosBrokerIdsOfLocality : oosBrokerIdsByLocality.entrySet()) {
      String oosLocality = oosBrokerIdsOfLocality.getKey();
      List<Integer> oosBrokerIds = oosBrokerIdsOfLocality.getValue();
      PriorityQueue<KafkaBroker> localityBrokerQueue = brokerQueueByLocality.get(oosLocality);
      Set<KafkaBroker> unusableBrokers =
          unusableBrokersByLocality.computeIfAbsent(oosLocality, l -> new HashSet<>());
      for( Integer oosBrokerId : oosBrokerIds){
        success = findNextBrokerForOosReplica(
            localityBrokerQueue,
            unusableBrokers,
            oosReplica.replicaBrokers,
            result,
            oosBrokerId,
            oosReplica.topicPartition,
            inBoundReq,
            outBoundReq,
            preferredBroker
        );

        // short circuit if failed to find available broker
        if (!success) {
          break;
        }
      }
      if (!success) {
        break;
      }
    }

    // maintain invariant
    for(Map.Entry<String, Set<KafkaBroker>> entry : unusableBrokersByLocality.entrySet()){
      brokerQueueByLocality.get(entry.getKey()).addAll(entry.getValue());
    }

    return success ? result : null;
  }

  /**
   * Finds the next broker in the broker queue for migrating a replica
   * @param brokerQueue a queue of brokers ordered by utilization
   * @param unusableBrokers the brokers that should not be used for reassignment
   * @param replicaBrokers the ids of the brokers that are already used for this replica
   * @param reassignmentMap the replica -> target broker mapping for the next reassignment
   * @param oosBrokerId the broker id of the current OutOfSync replica
   * @param tp the TopicPartition of the current replica
   * @param inBoundReq inbound traffic that needs to be reserved
   * @param outBoundReq outbound traffic that needs to be reserved
   * @param preferredBroker the preferred leader of the current TopicPartition
   * @return true if we successfully assigned a target broker for migration of this replica false otherwise
   */
  protected boolean findNextBrokerForOosReplica(
      PriorityQueue<KafkaBroker> brokerQueue,
      Collection<KafkaBroker> unusableBrokers,
      Collection<Integer> replicaBrokers,
      Map<Integer, KafkaBroker> reassignmentMap,
      Integer oosBrokerId,
      TopicPartition tp,
      Double inBoundReq,
      Double outBoundReq,
      Integer preferredBroker
  ){
    boolean success;
    KafkaBroker leastUsedBroker = brokerQueue.poll();
    while (leastUsedBroker != null && replicaBrokers.contains(leastUsedBroker.getId())) {
      unusableBrokers.add(leastUsedBroker);
      leastUsedBroker = brokerQueue.poll();
    }
    if (leastUsedBroker == null) {
      LOG.error("Failed to find a usable broker for fixing {}:{}", tp, oosBrokerId);
      success = false;
    } else {
      LOG.info("LeastUsedBroker for replacing {} : {}", oosBrokerId, leastUsedBroker.getId());
      success = preferredBroker == oosBrokerId ?
                leastUsedBroker.reserveBandwidth(tp, inBoundReq, outBoundReq) :
                leastUsedBroker.reserveBandwidth(tp, inBoundReq, 0);
      if (success) {
        reassignmentMap.put(oosBrokerId, leastUsedBroker);
        // the broker should not be used again for this topic partition.
        unusableBrokers.add(leastUsedBroker);
      } else {
        LOG.error("Failed to allocate resource to replace {}:{}", tp, oosBrokerId);
      }
    }
    return success;
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
    LOG.info("LeastUsedBroker for replacing {} : {}", topicPartition, leastUsedBroker.getId());
    boolean success = leastUsedBroker.reserveBandwidth(topicPartition, tpBytesIn, tpBytesOut);

    if (!success) {
      LOG.error("Failed to allocate resource to replace {}", topicPartition);
      return null;
    } else {
      return leastUsedBroker;
    }
  }

  public long getMaxBytesIn(TopicPartition tp) {
    return bytesInHistograms.get(tp).getSnapshot().getMax();
  }

  public long getMaxBytesOut(TopicPartition tp) {
    return bytesOutHistograms.get(tp).getSnapshot().getMax();
  }

  public long getMaxBytesIn() {
    long result = 0L;
    for (Map.Entry<String, Set<TopicPartition>> entry : topicPartitions.entrySet()) {
      Set<TopicPartition> topicPartitions = entry.getValue();
      for (TopicPartition tp : topicPartitions) {
        result += getMaxBytesIn(tp);
      }
    }
    return result;
  }

  public long getMaxBytesOut() {
    long result = 0L;
    for (Map.Entry<String, Set<TopicPartition>> entry : topicPartitions.entrySet()) {
      Set<TopicPartition> topicPartitions = entry.getValue();
      for (TopicPartition tp : topicPartitions) {
        result += getMaxBytesOut(tp);
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
