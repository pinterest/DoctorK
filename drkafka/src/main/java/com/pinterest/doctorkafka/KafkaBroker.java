package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private Set<TopicPartition> leaderReplicas = new HashSet<>();
  private Set<TopicPartition> followerReplicas = new HashSet<>();
  private Set<TopicPartition>  toBeAddedReplicas = new HashSet<>();

  private double bytesInPerSecLimit;
  private double bytesOutPerSecLimit;

  // reserved bytes in and bytes out rate per second
  private long reservedBytesIn;
  private long reservedBytesOut;

  private KafkaCluster kafkaCluster;
  private AtomicBoolean isDecommissioned = new AtomicBoolean(false);

  public KafkaBroker(
      String zkUrl,
      KafkaCluster kafkaCluster,
      int brokerId,
      double bytesInPerSecLimit,
      double bytesOutPerSecLimit
  ) {
    this.zkUrl = zkUrl;
    this.brokerId = brokerId;
    this.bytesInPerSecLimit = bytesInPerSecLimit;
    this.bytesOutPerSecLimit = bytesOutPerSecLimit;
    this.kafkaCluster = kafkaCluster;
  }

  public long getMaxBytesIn() {
    long result = 0L;
    for (TopicPartition topicPartition : leaderReplicas) {
      result += kafkaCluster.getMaxBytesIn(topicPartition);
    }
    for (TopicPartition topicPartition : followerReplicas) {
      result += kafkaCluster.getMaxBytesIn(topicPartition);
    }
    return result;
  }


  public long getMaxBytesOut() {
    long result = 0L;
    for (TopicPartition topicPartition : leaderReplicas) {
      result += kafkaCluster.getMaxBytesOut(topicPartition);
    }
    return result;
  }

  public long getReservedBytesIn() {
    return reservedBytesIn;
  }

  public long getReservedBytesOut() {
    return reservedBytesOut;
  }

  @JsonProperty
  public int getId() {
    return this.brokerId;
  }

  @JsonProperty
  public String getName() {
    return brokerName;
  }

  @JsonProperty
  public int getPort() {
    return this.brokerPort;
  }

  public long getLastStatsTimestamp() {
    return latestStats == null ? 0 : latestStats.getTimestamp();
  }

  public void clearResourceAllocationCounters() {
    this.reservedBytesIn = 0L;
    this.reservedBytesOut = 0L;
    this.toBeAddedReplicas.clear();
  }

  @JsonIgnore
  protected void setLeaderReplicas(Set<TopicPartition> leaderReplicas) {
    this.leaderReplicas = leaderReplicas;
  }

  @JsonIgnore
  protected void setFollowerReplicas(Set<TopicPartition> followerReplicas) {
    this.followerReplicas= followerReplicas;
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

  @JsonIgnore
  public BrokerStats getLatestStats() {
    return latestStats;
  }

  @VisibleForTesting
  protected void setLatestStats(BrokerStats brokerStats){
    this.latestStats = brokerStats;
  }

  @JsonProperty
  public String getRackId(){
    return rackId;
  }

  @VisibleForTesting
  protected void setRackId(String rackId){
    this.rackId = rackId;
  }

  /**
   *
   * Broker Decommissioning:
   * Currently, a decommissioned broker
   * WILL:
   *  1. Be ignored when checking for dead brokers (and thus no replacement will happen)
   *  2. Be ignored during URP reassignments
   *  3. Still update stats so it can catch up if decommission is cancelled
   * WILL NOT:
   *  1. Reassign the partitions to other brokers, you have to do it manually
   *
   * Note: Decommissioning is ephemeral for now, state is not preserved in ZK, so if a restart happens,
   * we will have to do it again
   *
   */

  /**
   * Decommissions the broker
   * @return previous decommission state
   */
  public boolean decommission() {
    return this.isDecommissioned.getAndSet(true);
  }

  /**
   * Cancels decommission of the broker
   * @return previous decommission state
   */
  public boolean cancelDecommission() {
    return this.isDecommissioned.getAndSet(false);
  }

  @JsonProperty
  public boolean isDecommissioned() {
    return this.isDecommissioned.get();
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

    if (stats.getLeaderReplicas() != null) {
      setLeaderReplicas(stats.getLeaderReplicas()
          .stream()
          .map(tps -> new TopicPartition(tps.getTopic(), tps.getPartition()))
          .collect(Collectors.toSet())
      );
    }

    if (stats.getFollowerReplicas() != null ) {
      setFollowerReplicas(stats.getFollowerReplicas()
          .stream()
          .map(tps -> new TopicPartition(tps.getTopic(), tps.getPartition()))
          .collect(Collectors.toSet())
      );
    }
  }


  public boolean reserveBandwidth(TopicPartition tp, double inBound, double outBound){
    if (bytesInPerSecLimit > getMaxBytesIn() + reservedBytesIn + inBound &&
        bytesOutPerSecLimit > getMaxBytesOut() + reservedBytesOut + outBound) {
      reservedBytesIn += inBound;
      reservedBytesOut += outBound;
      toBeAddedReplicas.add(tp);
      return true;
    }
    return false;
  }

  /**
   * cancels reservation of bandwidth for a TopicPartition
   * @param tp TopicPartition to cancel reservation
   * @param inBound amount of in-bound traffic to remove from broker
   * @param outBound amount of out-bound traffic to remove from broker
   * @return if reserved bandwidth has been successfully removed from broker
   */
  public boolean removeReservedBandwidth(TopicPartition tp, double inBound, double outBound){
    if ( toBeAddedReplicas.contains(tp) ){
      reservedBytesIn -= inBound;
      reservedBytesOut -= outBound;
      toBeAddedReplicas.remove(tp);
      return true;
    }
    return false;
  }

  @Deprecated
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
  @Deprecated
  public boolean reserveOutBoundBandwidth(TopicPartition tp, double outBound) {
    if (bytesOutPerSecLimit > getMaxBytesOut() + reservedBytesOut + outBound) {
      reservedBytesOut += outBound;
      toBeAddedReplicas.add(tp);
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("brokerId:" + getId());
    sb.append("; rackId = " + rackId);
    sb.append("; stats : " + (latestStats == null ? "null" : latestStats));
    return sb.toString();
  }


  @Deprecated
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
