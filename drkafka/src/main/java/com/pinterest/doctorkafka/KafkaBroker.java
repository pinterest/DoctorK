package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaBroker implements Comparable<KafkaBroker> {

  private static final Logger LOG = LogManager.getLogger(KafkaBroker.class);
  private static final Gson gson = new Gson();
  private DoctorKafkaClusterConfig clusterConfig;
  private String zkUrl;
  private int brokerId;
  private String brokerName;
  private int brokerPort = 9092;
  private String rackId;
  private BrokerStats latestStats;
  private Set<TopicPartition> leaderReplicas;
  private Set<TopicPartition> followerReplicas;

  private double bytesInPerSecLimit;
  private double bytesOutPerSecLimit;

  // reserved bytes in and bytes out rate per second
  private long reservedBytesIn;
  private long reservedBytesOut;
  private Set<TopicPartition>  toBeAddedReplicas;

  public KafkaBroker(DoctorKafkaClusterConfig clusterConfig, int brokerId) {
    assert clusterConfig != null;
    this.zkUrl = clusterConfig.getZkUrl();
    this.brokerId = brokerId;
    this.latestStats = null;
    this.rackId = null;
    this.leaderReplicas = new HashSet<>();
    this.followerReplicas = new HashSet<>();
    this.toBeAddedReplicas = new HashSet<>();
    this.clusterConfig = clusterConfig;
    this.reservedBytesIn = 0L;
    this.reservedBytesOut = 0L;
    this.bytesInPerSecLimit = clusterConfig.getNetworkInLimitInBytes();
    this.bytesOutPerSecLimit = clusterConfig.getNetworkOutLimitInBytes();
  }

  public JsonElement toJson() {
    // Return a JSON representation of a Kafka Broker.  Sadly, not everything can be trivially added.
    JsonObject json = new JsonObject();
    json.add("brokerId", gson.toJsonTree(brokerId));
    json.add("brokerName", gson.toJsonTree(brokerName));
    json.add("rackId", gson.toJsonTree(rackId));
    json.add("bytesInPerSecLimit", gson.toJsonTree(bytesInPerSecLimit));
    json.add("bytesOutPerSecLimit", gson.toJsonTree(bytesOutPerSecLimit));
    json.add("maxBytesOut", gson.toJsonTree(getMaxBytesOut()));
    json.add("maxBytesIn", gson.toJsonTree(getMaxBytesIn()));
    return json;
  }

  public long getMaxBytesIn() {
    long result = 0L;
    for (TopicPartition topicPartition : leaderReplicas) {
      result += ReplicaStatsManager.getMaxBytesIn(zkUrl, topicPartition);
    }
    for (TopicPartition topicPartition : followerReplicas) {
      result += ReplicaStatsManager.getMaxBytesIn(zkUrl, topicPartition);
    }
    return result;
  }


  public long getMaxBytesOut() {
    long result = 0L;
    for (TopicPartition topicPartition : leaderReplicas) {
      result += ReplicaStatsManager.getMaxBytesOut(zkUrl, topicPartition);
    }
    return result;
  }

  public long getReservedBytesIn() {
    return reservedBytesIn;
  }

  public long getReservedBytesOut() {
    return reservedBytesOut;
  }

  public int id() {
    return this.brokerId;
  }

  public String name() {
    return brokerName;
  }

  public int port() {
    return this.brokerPort;
  }

  public long lastStatsTimestamp() {
    return latestStats == null ? 0 : latestStats.getTimestamp();
  }


  public boolean reserveInBoundBandwidth(TopicPartition tp, double inBound) {
    if (bytesInPerSecLimit > getMaxBytesIn() + reservedBytesIn + inBound) {
      reservedBytesIn += inBound;
      toBeAddedReplicas.add(tp);
      return true;
    }
    return false;
  }

  /**
   *  To reserve out-bound bandwidth, we first need to compute the current network outbound usage.
   *  When a partition reassignment happens, the outbound metric of leader prelicas will be high.
   *  Because of this, using the brokerstats based information will lead to inaccurate decision.
   *  One observation is that in-bound traffic is more stable than the out-bound traffic,
   *  and read/write ratio for topics usually does not change often. Because of this, we can
   *  use the in-bound traffic times read/write ratio to infer the required out-bound bandwidth
   *  for topic partitions.
   *
   * @param tp the topic partition for reserving outbound bandwidth
   * @param outBound  the outbound bandwidth requirements in bytes/second
   * @return whether the reservation is successful or not.
   */
  public boolean reserveOutBoundBandwidth(TopicPartition tp, double outBound) {
    if (bytesOutPerSecLimit > getMaxBytesOut() + reservedBytesOut + outBound) {
      reservedBytesOut += outBound;
      toBeAddedReplicas.add(tp);
      return true;
    }
    return false;
  }

  public List<TopicPartition> getLeaderTopicPartitions() {
    BrokerStats brokerStats = getLatestStats();
    if (brokerStats == null) {
      LOG.error("Failed to get brokerstats for {}:{}", clusterConfig.getClusterName(), brokerId);
      return null;
    }
    List<TopicPartition> topicPartitions = new ArrayList<>();
    brokerStats.getLeaderReplicas().stream().forEach(atp ->
        topicPartitions.add(new TopicPartition(atp.getTopic(), atp.getPartition())));
    return topicPartitions;
  }

  public List<TopicPartition> getFollowerTopicPartitions() {
    BrokerStats brokerStats = getLatestStats();
    if (brokerStats == null) {
      LOG.error("Failed to get brokerstats for {}:{}", clusterConfig.getClusterName(), brokerId);
      return null;
    }
    List<TopicPartition> topicPartitions = new ArrayList<>();
    brokerStats.getFollowerReplicas().stream().forEach(atp ->
        topicPartitions.add(new TopicPartition(atp.getTopic(), atp.getPartition())));
    return topicPartitions;
  }


  public void clearResourceAllocationCounters() {
    this.reservedBytesIn = 0L;
    this.reservedBytesOut = 0L;
    this.toBeAddedReplicas.clear();
  }

  /**
   *  Record the stats, and update the topic partition list based on the stats
   *
   *  @param stats the broker stats
   */
  public synchronized void update(BrokerStats stats) {
    if (stats == null
        || (latestStats != null && latestStats.getTimestamp() > stats.getTimestamp())
        || stats.getHasFailure()) {
      return;
    }

    brokerName = stats.getName();
    latestStats = stats;
    if (rackId == null) {
      rackId = stats.getRackId() != null ? stats.getRackId() : stats.getAvailabilityZone();
    }

    // TODO: handle null poniter expceiton properly
    leaderReplicas = stats.getLeaderReplicas().stream().map(tps ->
        new TopicPartition(tps.getTopic(), tps.getPartition())).collect(Collectors.toSet());

    followerReplicas = stats.getFollowerReplicas().stream().map(tps ->
        new TopicPartition(tps.getTopic(), tps.getPartition())).collect(Collectors.toSet());
  }

  /**
   *  This is used in partition reassignment. During the partition reassignment, we cannot
   *  put two replicas on the same broker.
   *
   *  @param tp the topic partition for examining
   *  @return whether the broker has a replica for the given topic partition.
   */
  public boolean hasTopicPartition(TopicPartition tp) {
    return leaderReplicas.contains(tp) || followerReplicas.contains(tp)
        || toBeAddedReplicas.contains(tp);
  }

  public BrokerStats getLatestStats() {
    return latestStats;
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("brokerId:" + id());
    sb.append("; rackId = " + rackId);
    sb.append("; stats : " + (latestStats == null ? "null" : latestStats));
    return sb.toString();
  }

  public int compareTo(KafkaBroker another) {
    double networkUsage = getMaxBytesIn() + getMaxBytesOut()
        + reservedBytesIn + reservedBytesOut;
    double anotherUsage = another.getMaxBytesIn() + another.getMaxBytesOut()
        + another.reservedBytesIn + another.reservedBytesOut;

    return (networkUsage < anotherUsage) ? -1 : (networkUsage > anotherUsage ? 1 : 0);
  }

  /**
   * We first compare the two brokers based on their network usages. If they have the same
   * network usage, we compare their disk usage.
   */
  public static class KafkaBrokerComparator implements Comparator<KafkaBroker> {

    @Override
    public int compare(KafkaBroker x, KafkaBroker y) {
      double xNetworkUsage = x.getMaxBytesIn() + x.getMaxBytesOut()
          + x.reservedBytesIn + x.reservedBytesOut;
      double yNetworkUsage = y.getMaxBytesIn() + y.getMaxBytesOut()
          + y.reservedBytesIn + y.reservedBytesOut;
      return (xNetworkUsage < yNetworkUsage) ? -1 : (xNetworkUsage > yNetworkUsage ? 1 : 0);
    }
  }
}
