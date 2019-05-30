package com.pinterest.doctorkafka.modules.operator.cluster.kafka;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.DoctorKafkaMetrics;
import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.modules.action.ReportOperation;
import com.pinterest.doctorkafka.modules.action.SendEvent;
import com.pinterest.doctorkafka.modules.action.cluster.kafka.ReassignPartition;
import com.pinterest.doctorkafka.modules.action.errors.ReportActionFailedException;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.util.OpenTsdbMetricConverter;
import com.pinterest.doctorkafka.util.OperatorUtil;
import com.pinterest.doctorkafka.util.OutOfSyncReplica;
import com.pinterest.doctorkafka.util.UnderReplicatedReason;

import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

public class URPReassignor extends KafkaOperator {
  private final static Logger LOG = LogManager.getLogger(URPReassignor.class);
  //The time-out for machine reboot etc.
  private static final long MAX_HOST_REBOOT_TIME_MS = 300000L;
  private static final long MAX_TIMEOUT_MS = 300000L;
  //The number of broker stats that we need to examine to tell if a broker dies or not.
  private static final int NUM_BROKER_STATS = 4;

  private static final String CONFIG_NAME = "urp_reassignor";

  private static final String CONFIG_RACK_AWARENESS_KEY = "rack.awareness.enabled";
  private static final String CONFIG_PROLONG_URP_ALERT_SECONDS_KEY = "prolong.urp.alert.seconds";
  private static final String CONFIG_NETWORK_BANDWIDTH_MAX_KEY = "network.bandwidth.max.mb";
  private static final String CONFIG_ALERT_COOLDOWN_SECONDS_KEY = "alert.cooldown.seconds";
  private static final String CONFIG_NOTIFY_COOLDOWN_SECONDS_KEY = "notify.cooldown.seconds";

  private static final boolean DEFAULT_RACK_AWARENESS = false;
  private static final int DEFAULT_PROLONG_URP_ALERT_SECONDS = 7200;
  private static final int DEFAULT_COOLDOWN_SECONDS = 1200;

  private boolean configRackAwarenessEnabled;
  private int configProlongURPAlertInSec;
  private long configNetworkBandwidthMaxMb;
  private int configAlertCooldownSeconds;
  private int configNotifyCooldownSeconds;

  private long prevProlongURPAlertTime = -1L;
  private long prevFailedToHandleURPAlertTime = -1L;
  private boolean foundUrps = false;
  private long firstSeenUrpsTimestamp = Long.MAX_VALUE;

  private Map<String, scala.collection.Map<Object, Seq<Object>>> topicPartitionAssignments = new HashMap<>();
  private List<MutablePair<KafkaBroker, TopicPartition>> reassignmentFailures = new ArrayList<>();

  private SendEvent sendEvent;
  private ReassignPartition reassignPartition;
  private ReportOperation reportOperation;

  public URPReassignor(SendEvent sendEvent, ReassignPartition reassignPartition, ReportOperation reportOperation){
    this.sendEvent = sendEvent;
    this.reassignPartition = reassignPartition;
    this.reportOperation = reportOperation;
  }

  @Override
  public String getConfigName() {
    return CONFIG_NAME;
  }

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
    configProlongURPAlertInSec = config.getInteger(
        CONFIG_PROLONG_URP_ALERT_SECONDS_KEY,
        DEFAULT_PROLONG_URP_ALERT_SECONDS
    );

    configRackAwarenessEnabled = config.getBoolean(CONFIG_RACK_AWARENESS_KEY, DEFAULT_RACK_AWARENESS);
    if (!config.containsKey(CONFIG_NETWORK_BANDWIDTH_MAX_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_NETWORK_BANDWIDTH_MAX_KEY + " for plugin " + this.getClass());
    }
    configNetworkBandwidthMaxMb = config.getLong(CONFIG_NETWORK_BANDWIDTH_MAX_KEY);
    configAlertCooldownSeconds = config.getInt(CONFIG_ALERT_COOLDOWN_SECONDS_KEY, DEFAULT_COOLDOWN_SECONDS);
    configNotifyCooldownSeconds = config.getInt(CONFIG_NOTIFY_COOLDOWN_SECONDS_KEY, DEFAULT_COOLDOWN_SECONDS);
  }

  @Override
  public boolean operate(KafkaContext ctx, KafkaState state) throws Exception {
    ctx.getKafkaCluster().clearResourceAllocationCounters();
    List<PartitionInfo> underReplicatedPartitions = state.getUnderReplicatedPartitions();
    if (underReplicatedPartitions.size() > 0) {
      // handle under-replicated partitions
      if (!foundUrps) {
        foundUrps = true;
        firstSeenUrpsTimestamp = System.currentTimeMillis();
      } else {
        // send out an alert if the cluster has been under-replicated for a while
        long now = System.currentTimeMillis();
        long underReplicatedTimeMills = now - firstSeenUrpsTimestamp;
        if (underReplicatedTimeMills > configProlongURPAlertInSec * 1000) {
          String title = ctx.getClusterName() + " has been under-replicated for > "
              + underReplicatedTimeMills + " seconds (" + underReplicatedPartitions.size() + ") under-replicated partitions";
          StringBuilder msgSB = new StringBuilder();
          for (PartitionInfo partitionInfo : underReplicatedPartitions) {
            msgSB.append(partitionInfo + "\n");
          }
          LOG.warn(title, msgSB.toString());
          if ((now - prevProlongURPAlertTime)/1000L > configAlertCooldownSeconds){
            sendEvent.alert(title, msgSB.toString());
            prevProlongURPAlertTime = now;
          }
        }
      }
      LOG.info("Under-replicated partitions in cluster {} : {}",
          ctx.getClusterName(), underReplicatedPartitions.size());

      handleUnderReplicatedPartitions(ctx, underReplicatedPartitions);
    } else {
      foundUrps = false;
      firstSeenUrpsTimestamp = Long.MAX_VALUE;
    }
    return false;
  }

  /**
   * There are a few causes for under-replicated partitions:
   *    1. dead brokers
   *    2. leader network saturation
   *    3. follower network saturation
   *
   * For topic partitions that are under-replicated:
   *    1. topic partition is under-replicated, but still have a live leader
   *    2. there is no leader for that partition
   *
   */
  protected void handleUnderReplicatedPartitions(KafkaContext ctx, List<PartitionInfo> urps) {
    LOG.info("Start handling under-replicated partitions for {}", ctx.getClusterName());
    this.topicPartitionAssignments.clear();

    // filter out topic partitions that have more replicas than what is required
    List<OutOfSyncReplica> oosReplicas = urps.stream().map(urp -> {
      TopicPartition tp = new TopicPartition(urp.topic(), urp.partition());
      OutOfSyncReplica oosReplica = new OutOfSyncReplica(urp);
      oosReplica.replicaBrokers = getReplicaAssignment(ctx.getZkUtils(), tp);
      return oosReplica;
    }).collect(Collectors.toList());

    Map<MutablePair<Integer, Integer>, UnderReplicatedReason> urpReasons = new HashMap<>();
    Map<MutablePair<Integer, Integer>, Integer> triedTimes = new HashMap<>();
    Set<Integer> downBrokers = new HashSet<>();
    KafkaCluster kafkaCluster = ctx.getKafkaCluster();
    for (OutOfSyncReplica oosReplica : oosReplicas) {
      int leaderId = (oosReplica.leader == null) ? -1 : oosReplica.leader.id();
      for (int oosBrokerId : oosReplica.outOfSyncBrokers) {
        KafkaBroker broker = ctx.getKafkaCluster().getBroker(oosBrokerId);
        MutablePair<Integer, Integer> nodePair = new MutablePair<>(oosBrokerId, leaderId);
        Integer times = triedTimes.get(nodePair);
        if (times == null) {
          times = 0;
          triedTimes.put(nodePair, times);
        }
        // We only want to try per nodePair three times
        if (!urpReasons.containsKey(nodePair) || times < 3) {
          UnderReplicatedReason reason;
          // Avoid pinging the bad hosts again and again, it's very time consuming to wait
          // for the SocketTimeout
          if (downBrokers.contains(oosBrokerId)) {
            reason = UnderReplicatedReason.FOLLOWER_FAILURE;
          } else if (downBrokers.contains(leaderId)) {
            reason = UnderReplicatedReason.LEADER_FAILURE;
          } else {
            reason = getUnderReplicatedReason(
                kafkaCluster,
                broker.getName(),
                broker.getPort(),
                oosBrokerId,
                leaderId,
                oosReplica.topicPartition);
            if (reason == UnderReplicatedReason.FOLLOWER_FAILURE) {
              downBrokers.add(oosBrokerId);
            } else if (reason == UnderReplicatedReason.LEADER_FAILURE) {
              downBrokers.add(leaderId);
            }
          }
          urpReasons.put(nodePair, reason);
          triedTimes.put(nodePair, times++);
        }
      }
    }
    LOG.info("URP Reasons: {}", urpReasons);

    boolean alertOnFailure = true;
    boolean followerFailureOnly = true;
    for (Map.Entry<MutablePair<Integer, Integer>, UnderReplicatedReason> entry : urpReasons
        .entrySet()) {
      UnderReplicatedReason reason = entry.getValue();
      followerFailureOnly &= (reason == UnderReplicatedReason.FOLLOWER_FAILURE);
    }
    LOG.info("Down brokers: " + downBrokers);

    // when a kafka broker dies, PartitionInfo.replicas only has node.id info for the dead broker.
    // node.host() returns null. Because of this, we need to find the broker id info based on
    // the broker stats history.
    if (followerFailureOnly) {
      Map<TopicPartition, Integer[]> replicasMap;
      replicasMap = generateReassignmentPlanForDeadBrokers(kafkaCluster, oosReplicas);

      if (replicasMap != null && !replicasMap.isEmpty()) {
        scala.collection.Map<TopicAndPartition, Seq<Object>> proposedAssignment =
            getAssignmentPlan(replicasMap);
        String jsonReassignmentData = ZkUtils.formatAsReassignmentJson(proposedAssignment);

        LOG.info("Reassignment plan: {}", jsonReassignmentData);
        reassignTopicPartitions(ctx, jsonReassignmentData);
        alertOnFailure = false;
      } else {
        LOG.error("Failed to generate a reassignment plan");
        OpenTsdbMetricConverter.incr(DoctorKafkaMetrics.HANDLE_URP_FAILURE, 1, "cluster=" + ctx.getZkUrl());
      }
    }

    if (alertOnFailure) {
      try {
        alertOnFailedToHandleURP(ctx.getClusterName(), urps, downBrokers);
      } catch (Exception e){
        LOG.error("Failed to alert FailedToHandleURP event", e);
      }
    }
  }


  protected void reassignTopicPartitions(
      KafkaContext ctx,
      String jsonReassignmentData) {
    String clusterName = ctx.getClusterName();
    if (!super.isDryRun()) {
      try{
        reassignPartition.reassign(clusterName, jsonReassignmentData);
        reportOperation.report(clusterName,
            "partition reassignment : " + jsonReassignmentData);
        String title = clusterName + " partition reassignment ";
        String content = "Assignment json: \n\n" + jsonReassignmentData;
        LOG.info(title, content);
        long now = System.currentTimeMillis();
        if ((now - prevProlongURPAlertTime) / 1000L > configNotifyCooldownSeconds){
          sendEvent.notify(title, content);
        }
      } catch (ReportActionFailedException e){
        LOG.error("Failed reporting action:", e);
      } catch (Exception e){
        LOG.error("Failed to reassign partitions: ", e);
      }
    } else {
      LOG.info("Dry run: cluster {} partition reassignment: {}", clusterName, jsonReassignmentData);
    }
  }


  protected scala.collection.Map<TopicAndPartition, Seq<Object>> getAssignmentPlan(
      Map<TopicPartition, Integer[]> replicasMap) {
    scala.collection.mutable.HashMap<TopicAndPartition, Seq<Object>> result =
        new scala.collection.mutable.HashMap<>();

    for (Map.Entry<TopicPartition, Integer[]> entry : replicasMap.entrySet()) {
      TopicPartition tp = entry.getKey();
      TopicAndPartition tap = new TopicAndPartition(tp.topic(), tp.partition());
      List<Object> objs = Arrays.asList(entry.getValue()).stream()
          .map(val -> (Object) val).collect(Collectors.toList());
      Seq<Object> replicas = JavaConverters.asScalaBuffer(objs).seq();
      result.put(tap, replicas);
    }

    assert replicasMap.size() == result.size();
    LOG.debug("replicaMap.size = {}, result.size = {}", replicasMap.size(), result.size());
    return result;
  }

  /**
   * Get the replica assignment for a given topic partition. This information should be retrieved
   * from zookeeper as topic metadata that we get from kafkaConsumer.listTopic() does not specify
   * the preferred leader for topic partitions.
   *
   * @param tp  topic partition
   * @return the list of brokers that host the replica
   */
  private List<Integer> getReplicaAssignment(ZkUtils zkUtils, TopicPartition tp) {
    scala.collection.Map<Object, Seq<Object>> replicaAssignmentMap =
        getReplicaAssignmentForTopic(zkUtils, tp.topic());

    scala.Option<Seq<Object>> replicasOption = replicaAssignmentMap.get(tp.partition());
    Seq<Object> replicas = replicasOption.get();
    List<Object> replicasList = scala.collection.JavaConverters.seqAsJavaList(replicas);
    return replicasList.stream().map(obj -> (Integer) obj).collect(Collectors.toList());
  }


  private scala.collection.Map<Object, Seq<Object>> getReplicaAssignmentForTopic(
      ZkUtils zkUtils, String topic) {
    if (topicPartitionAssignments.containsKey(topic)) {
      return topicPartitionAssignments.get(topic);
    }
    List<String> topics = new ArrayList<>();
    topics.add(topic);
    Seq<String> topicsSeq = scala.collection.JavaConverters.asScalaBuffer(topics).toSeq();

    scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> assignments;
    assignments = zkUtils.getPartitionAssignmentForTopics(topicsSeq);

    scala.collection.Map<Object, Seq<Object>> partitionAssignment = assignments.get(topic).get();
    topicPartitionAssignments.put(topic, partitionAssignment);
    return partitionAssignment;
  }

  /**
   * Generate reassignment plan for dead brokers,
   * current reassignment will fail (reassignments are all-or-none).
   */
  private Map<TopicPartition, Integer[]> generateReassignmentPlanForDeadBrokers(
      KafkaCluster kafkaCluster,
      List<OutOfSyncReplica> outOfSyncReplicas) {
    Map<TopicPartition, Integer[]> replicasMap = new HashMap<>();
    boolean success = true;
    boolean isLocalityAware = configRackAwarenessEnabled;

    Map<String, PriorityQueue<KafkaBroker>> brokerQueueByLocality = null;
    PriorityQueue<KafkaBroker> brokerQueue = null;
    if(isLocalityAware){
      brokerQueueByLocality = kafkaCluster.getBrokerQueueByLocality();
    } else {
      brokerQueue = kafkaCluster.getBrokerQueue();
    }

    for (OutOfSyncReplica oosReplica : outOfSyncReplicas) {

      double inBoundReq = kafkaCluster.getMaxBytesIn(oosReplica.topicPartition);
      double outBoundReq = kafkaCluster.getMaxBytesOut(oosReplica.topicPartition);
      int preferredBroker = oosReplica.replicaBrokers.get(0);

      Map<Integer, KafkaBroker> replacedNodes;
      replacedNodes = isLocalityAware
                      ? kafkaCluster.getAlternativeBrokersByLocality(
          brokerQueueByLocality,
          oosReplica,
          inBoundReq,
          outBoundReq,
          preferredBroker
      )
                      : kafkaCluster.getAlternativeBrokers(
                          brokerQueue,
                          oosReplica,
                          inBoundReq,
                          outBoundReq,
                          preferredBroker
                      );
      if (replacedNodes == null) {
        // current reassignment task fail immediately
        // if failed to reassign for one partition
        success = false;
        for (int oosBrokerId : oosReplica.outOfSyncBrokers) {
          KafkaBroker broker = kafkaCluster.getBroker(oosBrokerId);
          reassignmentFailures.add(new MutablePair<>(broker, oosReplica.topicPartition));
        }
        break;
      } else {
        List<Integer> replicas = oosReplica.replicaBrokers;
        Integer[] newReplicas = new Integer[replicas.size()];
        for (int i = 0; i < replicas.size(); i++) {
          int brokerId = replicas.get(i);
          newReplicas[i] = replacedNodes.containsKey(brokerId) ? replacedNodes.get(brokerId).getId()
                                                               : brokerId;
        }
        replicasMap.put(oosReplica.topicPartition, newReplicas);
      }
    }

    // clean up if there are partial success reassignments
    if ( !success && replicasMap.size() > 0){
      kafkaCluster.clearResourceAllocationCounters();
    }

    return success ? replicasMap : null;
  }

  /**
   *  Figure out the reason of replication lag for a specific replica. First we check
   *  if the broker is dead. If the broker is still alive, we will check if it is caused
   *  by network saturation.
   */
  public UnderReplicatedReason getUnderReplicatedReason(KafkaCluster kafkaCluster,
                                                        String brokerHost,
                                                        int kafkaPort,
                                                        int brokerId,
                                                        int leaderId,
                                                        TopicPartition tp) {
    UnderReplicatedReason reason = UnderReplicatedReason.UNKNOWN;
    if (brokerHost != null && isDeadBroker(kafkaCluster, brokerHost, kafkaPort, brokerId, tp)) {
      reason = UnderReplicatedReason.FOLLOWER_FAILURE;
    } else if (leaderId < 0) {
      LOG.error("No live leader {}:{}", brokerHost, brokerId);
      reason = UnderReplicatedReason.NO_LEADER_FAILURE;
    } else {
      KafkaBroker leaderBroker = kafkaCluster.getBroker(leaderId);
      // Leader might be bad as well
      BrokerStats brokerStats = kafkaCluster.getLatestBrokerStats(brokerId);
      if (leaderBroker != null && isDeadBroker(kafkaCluster, leaderBroker.getName(), kafkaPort, leaderId, tp)) {
        reason = UnderReplicatedReason.LEADER_FAILURE;
      } else if (isNetworkSaturated(brokerStats)) {
        reason = UnderReplicatedReason.LEADER_NETWORK_SATURATION;
      } else if (isNetworkSaturated(brokerStats)) {
        reason = UnderReplicatedReason.FOLLOWER_NETWORK_SATURATION;
      }
    }
    return reason;
  }/**
   * check if the node is dead. In a distributed work, it is hard to tell precisely if
   * a broker is dead or not. This method only returns true when we are sure that the broker
   * is not available.
   */
  private boolean isDeadBroker(KafkaCluster kafkaCluster, String host, int kafkaPort, int brokerId, TopicPartition tp) {
    if (OperatorUtil.pingKafkaBroker(host, kafkaPort, 5000)) {
      LOG.debug("Broker {} is alive as {}:9092 is reachable", brokerId, host);
      if (OperatorUtil.canFetchData(host, kafkaPort, tp.topic(), tp.partition())) {
        LOG.debug("We are able to fetch data from broker {}", brokerId);
        return false;
      } else {
        LOG.warn("We are not able to fetch data from broker {} topic {}, par {}",
            brokerId, tp.topic(), tp.partition());
        return true;
      }
    }
    // invariant: cannot ping host:9092. The host may be rebooting. we need to wait
    // for some time to see if the host comes up.
    long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
    KafkaBroker broker = kafkaCluster.brokers.get(brokerId);
    if (broker == null) {
      // if kafka operator does not see any stats report from that broker,
      // we will wait $MAX_HOST_REBOOT_TIME_MS to return true.
      return uptime < MAX_HOST_REBOOT_TIME_MS;
    }

    // If a healthy broker is terminated before it can report any failure to doctorkafka,
    // the last few brokers stats that kafkaoperator received would always be healthy stats.
    // Because of this, we cannot rely on brokerstats.hasFailure field alone to tell if broker
    // has failure or not.
    List<BrokerStats> brokerStatsList = kafkaCluster.getBrokerStatsList(brokerId);
    BrokerStats latestStats = broker.getLatestStats();
    long now = System.currentTimeMillis();

    // if broker port 9092 is not reachable, and we haven't received brokerstats for a while,
    // we will consider that this broker is dead.
    if (now - latestStats.getTimestamp() > MAX_TIMEOUT_MS) {
      LOG.info("Haven't received {} brokerstats info for {} seconds",
          brokerId, (now - latestStats.getTimestamp()) / 1000.0);
      return true;
    }

    // otherwise, we will check the broker stats, and conclude that the broker fails
    // if all latest @NUM_BROKER_STATS brokerstats indicate broker failure.
    boolean allStatsHaveFailure = true;
    for (BrokerStats brokerStats : brokerStatsList) {
      allStatsHaveFailure &= brokerStats.getHasFailure();
    }
    LOG.info("# brokerstats={}, allStatsHaveFailure={}", brokerStatsList.size(),
        allStatsHaveFailure);
    return brokerStatsList.size() == NUM_BROKER_STATS && allStatsHaveFailure;
  }


  private boolean isNetworkSaturated(BrokerStats brokerStats) {
    if (brokerStats == null) {
      return false;
    }
    long inOneMinuteRate = brokerStats.getLeadersBytesIn1MinRate();
    long outOneMinuteRate = brokerStats.getLeadersBytesOut1MinRate();
    return inOneMinuteRate + outOneMinuteRate > configNetworkBandwidthMaxMb;
  }

  protected void alertOnFailedToHandleURP(String clusterName,
                                     List<PartitionInfo> urps,
                                     Set<Integer> downBrokers) throws Exception{
    String title = "Failed to handle under-replicated partitions on " + clusterName
        + " (" + urps.size() + " under-replicated partitions)";
    StringBuilder sb = new StringBuilder();
    for (PartitionInfo partitionInfo : urps) {
      sb.append(partitionInfo + "\n");
    }
    if (reassignmentFailures != null && !reassignmentFailures.isEmpty()) {
      sb.append("Reassignment failure: \n");
      reassignmentFailures.stream().forEach(pair -> {
        KafkaBroker broker = pair.getKey();
        TopicPartition topicPartition = pair.getValue();
        sb.append("Broker : " + broker.getName() + ", " + topicPartition);
      });
    }
    if (downBrokers != null && !downBrokers.isEmpty()) {
      sb.append("Down brokers: \n");
      sb.append(downBrokers);
    }
    String content = sb.toString();
    LOG.warn(title, content);
    long now = System.currentTimeMillis();
    if ((now - prevFailedToHandleURPAlertTime) / 1000L > configAlertCooldownSeconds){
      sendEvent.alert(title, content);
      prevFailedToHandleURPAlertTime = now;
    }
  }
}
