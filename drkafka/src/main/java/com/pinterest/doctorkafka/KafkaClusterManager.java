package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.notification.Email;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.util.BrokerReplacer;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.OpenTsdbMetricConverter;
import com.pinterest.doctorkafka.util.OperatorUtil;
import com.pinterest.doctorkafka.util.OutOfSyncReplica;
import com.pinterest.doctorkafka.util.PreferredReplicaElectionInfo;
import com.pinterest.doctorkafka.util.ReassignmentInfo;
import com.pinterest.doctorkafka.util.UnderReplicatedReason;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.ACL;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 *  There are primarily three reasons for partition under-replication:
 *    1. network saturation on leader broker
 *    2. dead broker
 *    3. degraded hardware
 */
public class KafkaClusterManager implements Runnable {

  private static final Logger LOG = LogManager.getLogger(KafkaClusterManager.class);
  private static final Gson gson = new Gson();
  /**
   * The time-out for machine reboot etc.
   */
  private static final long MAX_HOST_REBOOT_TIME_MS = 300000L;
  private static final long MAX_TIMEOUT_MS = 300000L;
  private static final long MAX_HOST_REPLACEMENT_TIME_SECONDS = 1800L;

  /**
   *  The number of broker stats that we need to examine to tell if a broker dies or not.
   */
  private static final int NUM_BROKER_STATS = 4;

  private String zkUrl;
  private SecurityProtocol securityProtocol;
  private Map<String, String> consumerConfigs;
  private ZkUtils zkUtils;
  private KafkaCluster kafkaCluster = null;
  private DoctorKafkaConfig drkafkaConfig = null;
  private DoctorKafkaClusterConfig clusterConfig;
  private DoctorKafkaActionReporter actionReporter = null;
  private boolean stopped = true;
  private Thread thread = null;

  private List<PartitionInfo> underReplicatedPartitions = new ArrayList<>();
  private double bytesInLimit;
  private double bytesOutLimit;

  private Map<String, scala.collection.Map<Object, Seq<Object>>> topicPartitionAssignments = new HashMap<>();
  private List<MutablePair<KafkaBroker, TopicPartition>> reassignmentFailures = new ArrayList<>();
  private BrokerReplacer brokerReplacer;
  private ZookeeperClient zookeeperClient;

  /**
   * fields that are used for partition reassignments
   */
  private Map<TopicPartition, ReassignmentInfo> reassignmentMap = new HashMap<>();
  private Map<TopicPartition, PreferredReplicaElectionInfo> preferredLeaders = new HashMap<>();
  private AtomicBoolean maintenanceMode = new AtomicBoolean(false);

  public KafkaClusterManager(String zkUrl, KafkaCluster kafkaCluster,
                             DoctorKafkaClusterConfig clusterConfig,
                             DoctorKafkaConfig drkafkaConfig,
                             DoctorKafkaActionReporter actionReporter,
                             ZookeeperClient zookeeperClient) {
    assert clusterConfig != null;
    this.zkUrl = zkUrl;
    this.zkUtils = KafkaUtils.getZkUtils(zkUrl);
    this.securityProtocol = clusterConfig.getSecurityProtocol();
    this.consumerConfigs = clusterConfig.getConsumerConfigurations();
    this.kafkaCluster = kafkaCluster;
    this.clusterConfig = clusterConfig;
    this.drkafkaConfig = drkafkaConfig;
    this.actionReporter = actionReporter;
    this.bytesInLimit = clusterConfig.getNetworkInLimitInBytes();
    this.bytesOutLimit = clusterConfig.getNetworkOutLimitInBytes();
    this.zookeeperClient = zookeeperClient;
    if (clusterConfig.enabledDeadbrokerReplacement()) {
      this.brokerReplacer = new BrokerReplacer(drkafkaConfig.getBrokerReplacementCommand());
    }
  }

  public KafkaCluster getCluster() {
    return kafkaCluster;
  }

  public void start() {
    thread = new Thread(this);
    thread.setName("ClusterManager:" + getClusterName());
    thread.start();
  }

  public void stop() {
    stopped = true;
  }

  public JsonElement toJson() {
    // Return a JSON representation of a Kafka Cluster.
    JsonObject json = new JsonObject();
    KafkaConsumer kafkaConsumer = KafkaUtils.getKafkaConsumer(zkUrl, securityProtocol, consumerConfigs);
    json.addProperty("zkUrl", zkUrl);
    json.add("bytesInLimit", gson.toJsonTree(bytesInLimit));
    json.add("bytesOutLimit", gson.toJsonTree(bytesOutLimit));
    json.add("underReplicatedPartitions", gson.toJsonTree(underReplicatedPartitions));
    json.add("topicPartitionAssignments", gson.toJsonTree(topicPartitionAssignments));
    json.add("kafkaCluster", gson.toJsonTree(kafkaCluster.toJson()));
    json.add("topics", gson.toJsonTree(kafkaConsumer.listTopics()));
    return json;
  }

  public String getClusterName() {
    return clusterConfig.getClusterName();
  }

  public String getZkUrl() {
    return zkUrl;
  }

  public Map<String, scala.collection.Map<Object, Seq<Object>>>  getTopicPartitionAssignments() {
    return topicPartitionAssignments;
  }

  public int getClusterSize() {
    if (kafkaCluster == null) {
      LOG.error("kafkaCluster is null for {}", zkUrl);
    }
    return kafkaCluster.size();
  }

  public List<PartitionInfo> getUnderReplicatedPartitions() {
    return underReplicatedPartitions;
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
   * Get the replica assignment for a given topic partition. This information should be retrieved
   * from zookeeper as topic metadata that we get from kafkaConsumer.listTopic() does not specify
   * the preferred leader for topic partitions.
   *
   * @param tp  topic partition
   * @return the list of brokers that host the replica
   */
  private List<Integer> getReplicaAssignment(TopicPartition tp) {
    scala.collection.Map<Object, Seq<Object>> replicaAssignmentMap =
        getReplicaAssignmentForTopic(zkUtils, tp.topic());

    scala.Option<Seq<Object>> replicasOption = replicaAssignmentMap.get(tp.partition());
    Seq<Object> replicas = replicasOption.get();
    List<Object> replicasList = scala.collection.JavaConverters.seqAsJavaList(replicas);
    return replicasList.stream().map(obj -> (Integer) obj).collect(Collectors.toList());
  }


  /**
   * generate the partition reassignment plan for moving high-traffic leader replicas out.
   *
   * @param broker that broker whose network traffic is high that the setting limits
   * @param leaderReplicas  the leader replicas that the broker hosts
   * @param averageBytesIn  average bytes in per second
   * @param averageBytesOut average bytes out per second
   */
  private void generateLeadersReassignmentPlan(KafkaBroker broker,
                                               List<TopicPartition> leaderReplicas,
                                               double averageBytesIn,
                                               double averageBytesOut) {
    LOG.info("Start generating leader reassignment plan for {}", broker.name());
    if (leaderReplicas == null) {
      LOG.info("broker {} does not have leader partition", broker.id());
      return;
    }

    Map<TopicPartition, Double> tpTraffic = sortTopicPartitionsByTraffic(leaderReplicas);
    try {
      double bytesIn = broker.getMaxBytesIn() + broker.getReservedBytesIn();
      double bytesOut = broker.getMaxBytesOut() + broker.getReservedBytesOut();
      double toBeReducedBytesIn = 0.0;
      double toBeReducedBytesOut = 0.0;

      for (Map.Entry<TopicPartition, Double> entry : tpTraffic.entrySet()) {
        TopicPartition tp = entry.getKey();
        double tpBytesIn = ReplicaStatsManager.getMaxBytesIn(zkUrl, tp);
        double tpBytesOut = ReplicaStatsManager.getMaxBytesOut(zkUrl, tp);
        double brokerTraffic = (bytesIn - toBeReducedBytesIn - tpBytesIn) +
            (bytesOut - toBeReducedBytesOut - tpBytesOut);

        // do preferred-leader election if possible to minimize data movement
        LOG.info("checking tp {}", tp);
        List<Integer> replicasList = getReplicaAssignment(tp);

        // if the preferred leader is not the current leader,
        // check if applying preferred leader election is feasible
        int preferredBrokerId = replicasList.get(0);
        if (preferredBrokerId != broker.id()) {
          LOG.info("Partition {}: {}, broker :{}", tp.partition(), replicasList, broker.name());
          KafkaBroker another = kafkaCluster.getBroker(preferredBrokerId);
          // we only need to check if the outbound bandwidth for preferredBroker as
          // there will be no in-bound traffic change
          double anotherFutureBytesOut = another.getMaxBytesOut() + another.getReservedBytesOut();
          if (anotherFutureBytesOut + tpBytesOut <= bytesOutLimit) {
            PreferredReplicaElectionInfo preferredLeader;
            preferredLeader = new PreferredReplicaElectionInfo(tp, preferredBrokerId);
            preferredLeaders.put(tp, preferredLeader);
            toBeReducedBytesOut += tpBytesOut;
            another.reserveOutBoundBandwidth(tp, tpBytesOut);
            continue;
          }
        } else if (brokerTraffic < averageBytesIn + averageBytesOut) {
          // if moving a replica out will have the broker be under-utilized, do not move it out.
          continue;
        }

        // invariant: is preferred leader, and moving this replica out will be helpful.
        KafkaBroker alterBroker = kafkaCluster.getAlternativeBroker(tp, tpBytesIn, tpBytesOut);
        if (alterBroker != null) {
          LOG.info("Alternative broker for {} : {} -> {}", tp, broker.name(), alterBroker.name());
          LOG.info("    tpBytesIn:{}, tpBytesOut:{}", tpBytesIn, tpBytesOut);
          LOG.info("    to be added: in: {}, out: {}", alterBroker.getReservedBytesIn(),
              alterBroker.getReservedBytesOut());

          ReassignmentInfo reassign = new ReassignmentInfo(tp, broker, alterBroker);
          reassignmentMap.put(tp, reassign);
          LOG.info("    {} : {} -> {}", tp, reassign.source.name(), reassign.dest.name());
          toBeReducedBytesIn += tpBytesIn;
          toBeReducedBytesOut += tpBytesOut;

          if (bytesIn - toBeReducedBytesIn <= bytesInLimit &&
              bytesOut - toBeReducedBytesOut <= bytesOutLimit) {
            break;
          }
        } else {
          LOG.info("Could not find an alternative broker for {}:{} ", broker.name(), tp);
          reassignmentFailures.add(new MutablePair<>(broker, tp));
        }
      }
    } catch (Exception e) {
      LOG.info("Failure in generating leader assignment plan for {}", broker.name(), e);
    }
    LOG.info("End generating leader reassignment plan for {}", broker.name());
  }


  /**
   *  Reassign the follower partitions
   */
  private void generateFollowerReassignmentPlan(KafkaBroker broker) {
    LOG.info("Begin generating follower reassignment plan for {}", broker.name());
    List<TopicPartition> topicPartitions = broker.getFollowerTopicPartitions();
    Map<TopicPartition, Double> tpTraffic = sortTopicPartitionsByTraffic(topicPartitions);

    tpTraffic.keySet().stream().forEach(
        tp -> LOG.info("     traffic :{} : {}", tp, tpTraffic.get(tp)));

    try {
      double brokerBytesIn = broker.getMaxBytesIn() + broker.getReservedBytesIn();
      double toBeReducedBytesIn = 0.0;

      for (Map.Entry<TopicPartition, Double> entry : tpTraffic.entrySet()) {
        TopicPartition tp = entry.getKey();
        double tpBytesIn = ReplicaStatsManager.getMaxBytesIn(zkUrl, tp);
        if (brokerBytesIn - toBeReducedBytesIn - tpBytesIn < bytesInLimit) {
          // if moving a topic partition out will have the broker be under-utilized, do not
          // move it out.
          continue;
        }
        KafkaBroker alterBroker = kafkaCluster.getAlternativeBroker(tp, tpBytesIn, 0);
        if (alterBroker != null) {
          LOG.info("  Alternative broker for {} : {} -> {}, bytesIn: {}", tp, broker.name(),
              alterBroker.name(), tpBytesIn);
          ReassignmentInfo reassign = new ReassignmentInfo(tp, broker, alterBroker);
          reassignmentMap.put(tp, reassign);
          LOG.info("    {} : {} -> {}", tp, reassign.source.name(), reassign.dest.name());
          toBeReducedBytesIn += tpBytesIn;
          if (broker.getMaxBytesIn() - toBeReducedBytesIn <= bytesInLimit) {
            break;
          }
        } else {
          LOG.info("Could not find an alternative broker for {}:{}", broker.name(), tp);
          reassignmentFailures.add(new MutablePair<>(broker, tp));
        }
      }
    } catch (Exception e) {
      LOG.info("Exception in generating follower reassignment plan", e);
    }
  }


  private Map<String, List<PartitionInfo>> getTopicPartitionInfoMap() {
    KafkaConsumer kafkaConsumer = KafkaUtils.getKafkaConsumer(zkUrl, securityProtocol, consumerConfigs);
    Map<String, List<PartitionInfo>> topicPartitonInfoMap = kafkaConsumer.listTopics();
    return topicPartitonInfoMap;
  }


  public List<KafkaBroker> getHighTrafficBroker() {
    List<KafkaBroker> highTrafficBrokers = kafkaCluster.getHighTrafficBrokers();
    Collections.sort(highTrafficBrokers);
    Collections.reverse(highTrafficBrokers);
    for (KafkaBroker broker : highTrafficBrokers) {
      LOG.info("high traffic borker: {} : [{}, {}]",
          broker.name(), broker.getMaxBytesIn(), broker.getMaxBytesOut());
    }
    return highTrafficBrokers;
  }


  /**
   * Generate the workload balancing plan in json.
   */
  public String getWorkloadBalancingPlanInJson(List<KafkaBroker> highTrafficBrokers) {
    kafkaCluster.clearResourceAllocationCounters();
    reassignmentFailures.clear();

    Map<String, List<PartitionInfo>> topicPartitonInfoMap = getTopicPartitionInfoMap();
    Map<Integer, List<TopicPartition>> leaderTopicPartitions =
        getBrokerLeaderPartitions(topicPartitonInfoMap);

    double averageBytesIn = kafkaCluster.getMaxBytesIn() / (double) kafkaCluster.size();
    double averageBytesOut = kafkaCluster.getMaxBytesOut() / (double) kafkaCluster.size();
    LOG.info("Cluster {}: bytesInAvg={}, bytesOutAvg={}", zkUrl, averageBytesIn, averageBytesOut);

    for (KafkaBroker broker : highTrafficBrokers) {
      try {
        if (broker.getMaxBytesOut() > clusterConfig.getNetworkOutLimitInBytes()) {
          // need to move some leader partitions out, or switch preferred leaders
          List<TopicPartition> leaderReplicas = leaderTopicPartitions.get(broker.id());
          generateLeadersReassignmentPlan(broker, leaderReplicas, averageBytesIn, averageBytesOut);
        } else if (broker.getMaxBytesIn() > clusterConfig.getNetworkInLimitInBytes()) {
          // move some followers out may be sufficient
          generateFollowerReassignmentPlan(broker);
        }
      } catch (Exception e) {
        LOG.info("Exception in generating assignment plan for {}", broker.name(), e);
      }
    }

    if (!reassignmentFailures.isEmpty()) {
      return null;
    }

    LOG.info("Printing reassignment map + preferred leaders.");
    reassignmentMap.values().stream().forEach(reassign -> LOG.info(reassign));
    preferredLeaders.values().stream().forEach(replica -> LOG.info(replica));
    LOG.info("End of printing reassignment map + preferred leaders.");

    if (!preferredLeaders.isEmpty()) {
      scala.collection.mutable.Set<TopicAndPartition> tpSet =
          new scala.collection.mutable.HashSet<>();
      for (PreferredReplicaElectionInfo preferred : preferredLeaders.values()) {
        TopicPartition tp = preferred.topicPartition;
        TopicAndPartition tap = new TopicAndPartition(tp.topic(), tp.partition());
        tpSet.add(tap);
      }
      String jsonData = ZkUtils.preferredReplicaLeaderElectionZkData(tpSet);
      List<ACL> acls = KafkaUtils.getZookeeperAcls(false);

      if (!zkUtils.pathExists(KafkaUtils.PreferredReplicaLeaderElectionPath)) {
        zkUtils.createPersistentPath(KafkaUtils.PreferredReplicaLeaderElectionPath, jsonData, acls);
      }
    }

    Map<String, Map<Integer, PartitionInfo>> tpInfoMap = new HashMap<>();
    for (String topic : topicPartitonInfoMap.keySet()) {
      List<PartitionInfo> partitionInfos = topicPartitonInfoMap.get(topic);
      tpInfoMap.putIfAbsent(topic, new HashMap<>());
      Map<Integer, PartitionInfo> partitionInfoMap = tpInfoMap.get(topic);
      for (PartitionInfo partitionInfo : partitionInfos) {
        partitionInfoMap.put(partitionInfo.partition(), partitionInfo);
      }
    }

    Map<TopicPartition, Integer[]> assignmentPlan = new HashMap<>();
    // limit to reassign one partition per broker at a time to reduce congestion
    Set<Integer> sourceBrokerId = new HashSet<>();

    for (TopicPartition tp : reassignmentMap.keySet()) {
      ReassignmentInfo reassign = reassignmentMap.get(tp);
      PartitionInfo partitionInfo = tpInfoMap.get(tp.topic()).get(tp.partition());

      Node[] replicas = partitionInfo.replicas();
      Integer[] newReplicas = new Integer[partitionInfo.replicas().length];
      for (int i = 0; i < replicas.length; i++) {
        if (replicas[i].id() == reassign.source.id()) {
          newReplicas[i] = reassign.dest.id();
        } else {
          newReplicas[i] = replicas[i].id();
        }
      }
      assignmentPlan.put(tp, newReplicas);
      sourceBrokerId.add(reassign.source.id());
    }
    if (assignmentPlan.size() > 0) {
      scala.collection.Map<TopicAndPartition, Seq<Object>> proposedAssignment =
          getAssignmentPlan(assignmentPlan);
      String jsonReassignmentData = ZkUtils.formatAsReassignmentJson(proposedAssignment);
      return jsonReassignmentData;
    } else {
      return "";
    }
  }


  /**
   *  let  load_avg  be the average broker workload
   *  for each broker b :  workload(b) \> load_avg * (1 + max_variance):
   *    while workload(b)  load_avg * (1 + max_variance):
   *      select a batch of leader replicas on broker b
   *      for each leader replica tp in the batch:
   *        if  exists follower boker c that has capacity to host tp as leader:
   *          add [tp,  b → c]  to the leader movement list
   *        else if exist borker h that satisfies constraints for hosting tp:
   *          add [tp → h] to partition reassignment list
   *        else:
   *          send out alerts and exit
   *
   *  execute leader movement and partition reassignment
   */
  public void balanceWorkload() {
    List<KafkaBroker> highTrafficBrokers = getHighTrafficBroker();
    String reassignmentPlan = getWorkloadBalancingPlanInJson(highTrafficBrokers);
    if (reassignmentPlan != null && !reassignmentPlan.isEmpty()) {
      LOG.info("Assignment plan: {}" + reassignmentPlan);
      reassignTopicPartitions(reassignmentPlan);
    } else {
      //TODO: send out alerts on failure in balancing load
    }
  }


  private Map<TopicPartition, Double> sortTopicPartitionsByTraffic(List<TopicPartition> tps) {
    // sort the topic partitions based on traffic in descending order
    Map<TopicPartition, Double> tpTraffic = new HashMap<>();
    for (TopicPartition tp : tps) {
      try {
        double bytesIn = ReplicaStatsManager.getMaxBytesIn(zkUrl, tp);
        double bytesOut = ReplicaStatsManager.getMaxBytesOut(zkUrl, tp);
        tpTraffic.put(tp, bytesIn + bytesOut);
      } catch (Exception e) {
        LOG.info("Exception in sorting topic partition {}", tp, e);
      }
    }
    tpTraffic = OperatorUtil.sortByValue(tpTraffic);
    return tpTraffic;
  }


  private void reassignTopicPartitions(String jsonReassignmentData) {
    if (zkUtils.pathExists(KafkaUtils.ReassignPartitionsPath)) {
      LOG.warn("{} : There is an existing assignment.", clusterConfig.getClusterName());
    } else if (!clusterConfig.dryRun()) {
      List<ACL> acls = KafkaUtils.getZookeeperAcls(false);
      zkUtils.createPersistentPath(KafkaUtils.ReassignPartitionsPath, jsonReassignmentData, acls);
      LOG.info("Set the reassignment data: ");
      actionReporter.sendMessage(clusterConfig.getClusterName(),
          "partition reassignment : " + jsonReassignmentData);
      Email.notifyOnPartitionReassignment(drkafkaConfig.getNotificationEmails(),
          clusterConfig.getClusterName(), jsonReassignmentData);
    }
  }


  private scala.collection.Map<TopicAndPartition, Seq<Object>> getAssignmentPlan(
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
   *  Remove the under-replicated partitions that are in the middle of partition reassignment.
   */
  public List<PartitionInfo> filterOutInReassignmentUrps(List<PartitionInfo> urps,
                                                         Map<String, Integer> replicationFactors) {
    List<PartitionInfo> result = new ArrayList<>();
    for (PartitionInfo urp : urps) {
      if (urp.replicas().length <= replicationFactors.get(urp.topic())) {
        // # of replicas <= replication factor
        result.add(urp);
      } else {
        // # of replicas > replication factor. this can happen after
        // a failed partition reassignment
        Set<Integer> liveReplicas = new HashSet<>();
        for (Node node : urp.replicas()) {
          if (node.host() != null && OperatorUtil.pingKafkaBroker(node.host(), 9092, 5000)) {
            liveReplicas.add(node.id());
          }
        }
        if (liveReplicas.size() < replicationFactors.get(urp.topic())) {
          result.add(urp);
        }
      }
    }
    return result;
  }

  /**
   *  Figure out the reason of replication lag for a specific replica. First we check
   *  if the broker is dead. If the broker is still alive, we will check if it is caused
   *  by network saturation.
   */
  public UnderReplicatedReason getUnderReplicatedReason(String brokerHost,
                                                        int kafkaPort,
                                                        int brokerId,
                                                        int leaderId,
                                                        TopicPartition tp) {
    UnderReplicatedReason reason = UnderReplicatedReason.UNKNOWN;
    if (brokerHost != null && isDeadBroker(brokerHost, kafkaPort, brokerId, tp)) {
      reason = UnderReplicatedReason.FOLLOWER_FAILURE;
    } else if (leaderId < 0) {
      LOG.error("No live leader {}:{}", brokerHost, brokerId);
      reason = UnderReplicatedReason.NO_LEADER_FAILURE;
    } else {
      KafkaBroker leaderBroker = kafkaCluster.getBroker(leaderId);
      // Leader might be bad as well
      if (leaderBroker != null && isDeadBroker(leaderBroker.name(), kafkaPort, leaderId, tp)) {
        reason = UnderReplicatedReason.LEADER_FAILURE;
      } else if (isNetworkSaturated(leaderId)) {
        reason = UnderReplicatedReason.LEADER_NETWORK_SATURATION;
      } else if (isNetworkSaturated(leaderId)) {
        reason = UnderReplicatedReason.FOLLOWER_NETWORK_SATURATION;
      }
    }
    return reason;
  }


  /**
   * Generate reassignment plan for dead brokers
   */
  private Map<TopicPartition, Integer[]> generateReassignmentPlanForDeadBrokers(
      List<OutOfSyncReplica> outOfSyncReplicas) {
    Map<TopicPartition, Integer[]> replicasMap = new HashMap<>();
    boolean success = true;

    PriorityQueue<KafkaBroker> brokerQueue = kafkaCluster.getBrokerQueue();
    for (OutOfSyncReplica oosReplica : outOfSyncReplicas) {
      Map<Integer, KafkaBroker> replacedNodes =
          kafkaCluster.getAlternativeBrokers(brokerQueue, oosReplica);
      if (replacedNodes == null) {
        success = false;
        for (int oosBrokerId : oosReplica.outOfSyncBrokers) {
          KafkaBroker broker = kafkaCluster.getBroker(oosBrokerId);
          reassignmentFailures.add(new MutablePair(broker, oosReplica.topicPartition));
        }
        break;
      } else {
        List<Integer> replicas = oosReplica.replicaBrokers;
        Integer[] newReplicas = new Integer[replicas.size()];
        for (int i = 0; i < replicas.size(); i++) {
          int brokerId = replicas.get(i);
          newReplicas[i] = replacedNodes.containsKey(brokerId) ? replacedNodes.get(brokerId).id()
                                                               : brokerId;
        }
        replicasMap.put(oosReplica.topicPartition, newReplicas);
      }
    }
    return success ? replicasMap : null;
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
  public void handleUnderReplicatedPartitions(List<PartitionInfo> initialUrps,
                                              Map<String, Integer> replicationFactors) {
    LOG.info("Start handling under-replicated partitions for {}", clusterConfig.getClusterName());
    this.topicPartitionAssignments.clear();

    // filter out topic partitions that have more replicas than what is required
    List<PartitionInfo> urps = filterOutInReassignmentUrps(initialUrps, replicationFactors);
    List<OutOfSyncReplica> oosReplicas = urps.stream().map(urp -> {
      TopicPartition tp = new TopicPartition(urp.topic(), urp.partition());
      OutOfSyncReplica oosReplica = new OutOfSyncReplica(urp);
      oosReplica.replicaBrokers = getReplicaAssignment(tp);
      return oosReplica;
    }).collect(Collectors.toList());

    Map<MutablePair<Integer, Integer>, UnderReplicatedReason> urpReasons = new HashMap<>();
    Map<MutablePair<Integer, Integer>, Integer> triedTimes = new HashMap<>();
    Set<Integer> downBrokers = new HashSet<>();
    for (OutOfSyncReplica oosReplica : oosReplicas) {
      int leaderId = (oosReplica.leader == null) ? -1 : oosReplica.leader.id();
      for (int oosBrokerId : oosReplica.outOfSyncBrokers) {
        KafkaBroker broker = kafkaCluster.getBroker(oosBrokerId);
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
            reason = getUnderReplicatedReason(broker.name(), broker.port(), oosBrokerId, leaderId,
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
      MutablePair<Integer, Integer> pair = entry.getKey();
    }
    LOG.info("Down brokers: " + downBrokers);

    // when a kafka broker dies, PartitionInfo.replicas only has node.id info for the dead broker.
    // node.host() returns null. Because of this, we need to find the broker id info based on
    // the broker stats history.
    if (followerFailureOnly) {
      Map<TopicPartition, Integer[]> replicasMap;
      replicasMap = generateReassignmentPlanForDeadBrokers(oosReplicas);

      if (replicasMap != null && !replicasMap.isEmpty()) {
        scala.collection.Map<TopicAndPartition, Seq<Object>> proposedAssignment =
            getAssignmentPlan(replicasMap);
        String jsonReassignmentData = ZkUtils.formatAsReassignmentJson(proposedAssignment);

        LOG.info("Reassignment plan: {}", jsonReassignmentData);
        reassignTopicPartitions(jsonReassignmentData);
        alertOnFailure = false;
      } else {
        LOG.error("Failed to generate a reassignment plan");
        OpenTsdbMetricConverter.incr(DoctorKafkaMetrics.HANDLE_URP_FAILURE, 1, "cluster=" + zkUrl);
      }
    }

    if (alertOnFailure) {
      Email.alertOnFailureInHandlingUrps(drkafkaConfig.getNotificationEmails(),
          clusterConfig.getClusterName(), urps, reassignmentFailures, downBrokers);
    }
  }


  /**
   * check if the node is dead. In a distributed work, it is hard to tell precisely if
   * a broker is dead or not. This method only returns true when we are sure that the broker
   * is not available.
   */
  private boolean isDeadBroker(String host, int kafkaPort, int brokerId, TopicPartition tp) {
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


  private boolean isNetworkSaturated(int brokerId) {
    BrokerStats brokerStats = kafkaCluster.getLatestBrokerStats(brokerId);
    if (brokerStats == null) {
      return false;
    }
    long inOneMinuteRate = brokerStats.getLeadersBytesIn1MinRate();
    long outOneMinuteRate = brokerStats.getLeadersBytesOut1MinRate();
    return inOneMinuteRate + outOneMinuteRate > clusterConfig.getNetworkBandwidthInBytes();
  }


  public Map<Integer, List<TopicPartition>> getBrokerLeaderPartitions(
      Map<String, List<PartitionInfo>> topicPartitonInfoMap) {
    Map<Integer, List<TopicPartition>> result = new HashMap<>();

    for (String topic : topicPartitonInfoMap.keySet()) {
      List<PartitionInfo> partitionInfoList = topicPartitonInfoMap.get(topic);
      if (partitionInfoList == null) {
        LOG.error("Failed to get partition info for {}", topic);
        continue;
      }

      for (PartitionInfo info : partitionInfoList) {
        Node leaderNode = info.leader();
        if (leaderNode != null) {
          result.putIfAbsent(leaderNode.id(), new ArrayList<>());
          TopicPartition topicPartiton = new TopicPartition(info.topic(), info.partition());
          result.get(leaderNode.id()).add(topicPartiton);
        }
      }
    }
    return result;
  }


  /**
   * Call the kafka api to get the list of under-replicated partitions.
   * When a topic partition loses all of its replicas, it will not have a leader broker.
   * We need to handle this special case in detecting under replicated topic partitions.
   */
  public static List<PartitionInfo> getUnderReplicatedPartitions(
      String zkUrl, SecurityProtocol securityProtocol, Map<String, String> consumerConfigs,
      List<String> topics,
      scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignments,
      Map<String, Integer> replicationFactors,
      Map<String, Integer> partitionCounts) {
    List<PartitionInfo> underReplicated = new ArrayList();
    KafkaConsumer kafkaConsumer = KafkaUtils.getKafkaConsumer(zkUrl, securityProtocol, consumerConfigs);
    for (String topic : topics) {
      List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
      if (partitionInfoList == null) {
        LOG.error("Failed to get partition info for {}", topic);
        continue;
      }
      int numPartitions = partitionCounts.get(topic);

      // when a partition loses all replicas and does not have a live leader,
      // kafkaconsumer.partitionsFor(...) will not return info for that partition.
      // the noLeaderFlag array is used to detect partitions that have no leaders
      boolean[] noLeaderFlags = new boolean[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        noLeaderFlags[i] = true;
      }
      for (PartitionInfo info : partitionInfoList) {
        if (info.inSyncReplicas().length < info.replicas().length &&
            replicationFactors.get(info.topic()) > info.inSyncReplicas().length) {
          underReplicated.add(info);
        }
        noLeaderFlags[info.partition()] = false;
      }

      // deal with the partitions that do not have leaders
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        if (noLeaderFlags[partitionId]) {
          Seq<Object> seq = partitionAssignments.get(topic).get().get(partitionId).get();
          Node[] nodes = JavaConverters.seqAsJavaList(seq).stream()
              .map(val -> new Node((Integer) val, "", -1)).toArray(Node[]::new);
          PartitionInfo partitionInfo =
              new PartitionInfo(topic, partitionId, null, nodes, new Node[0]);
          underReplicated.add(partitionInfo);
        }
      }
    }
    return underReplicated;
  }

  /**
   *   return the list of brokers that do not have stats
   */
  public List<Broker> getNoStatsBrokers() {
    Seq<Broker> brokerSeq = zkUtils.getAllBrokersInCluster();
    List<Broker> brokers = scala.collection.JavaConverters.seqAsJavaList(brokerSeq);
    List<Broker> noStatsBrokers = new ArrayList<>();

    brokers.stream().forEach(broker -> {
      if (kafkaCluster.getBroker(broker.id()) == null) {
        noStatsBrokers.add(broker);
      }
    });
    return noStatsBrokers;
  }

  public List<KafkaBroker> getAllBrokers() {
    return new ArrayList<>(kafkaCluster.brokers.values());
  }


  private boolean checkAndReplaceDeadBrokers() {
    long now = System.currentTimeMillis();
    if (brokerReplacer.busy()) {
      long brokerReplacementTimeInSeconds = (now - brokerReplacer.getReplacementStartTime()) / 1000;
      if (brokerReplacementTimeInSeconds > MAX_HOST_REPLACEMENT_TIME_SECONDS) {
        // send out alerts for prolonged broker replacement
        Email.alertOnProlongedBrokerReplacement(drkafkaConfig.getNotificationEmails(),
            clusterConfig.getClusterName(), brokerReplacer.getReplacedBroker(),
            brokerReplacementTimeInSeconds);
      }
      LOG.info("{} broker replacer is busy with replacing {}", clusterConfig.getClusterName(),
          brokerReplacer.getReplacedBroker());
      return false;
    }

    KafkaBroker toBeReplaced = null;
    for (Map.Entry<Integer, KafkaBroker> brokerEntry : kafkaCluster.brokers.entrySet()) {
      KafkaBroker broker = brokerEntry.getValue();
      double lastUpdateTime = (now - broker.lastStatsTimestamp()) / 1000.0;
      // call broker replacement script to replace dead brokers
      if (lastUpdateTime > clusterConfig.getBrokerReplacementNoStatsSeconds()) {
        toBeReplaced = broker;
        break;
      }
    }

    if (toBeReplaced != null) {
      String brokerName= toBeReplaced.name();
      String clusterName = clusterConfig.getClusterName();

      try {
        long lastReplacementTime =
            zookeeperClient.getLastBrokerReplacementTime(clusterConfig.getClusterName());
        long elaspedTimeInSeconds = (now - lastReplacementTime) / 1000;
        if (elaspedTimeInSeconds < drkafkaConfig.getBrokerReplacementIntervalInSeconds()) {
          LOG.info("Cannot replace {}:{} due to replace frequency limitation",
              clusterName, brokerName);
          return false;
        }
      } catch (Exception e) {
        LOG.error("Failed to check last broker replacement info for {}", clusterName, e);
        return false;
      }

      LOG.info("Replacing {}  in {}", brokerName, clusterName);
      brokerReplacer.replaceBroker(brokerName);
      zookeeperClient.recordBrokerTermination(clusterName, brokerName);
      actionReporter.sendMessage(clusterName, "broker replacement : " + brokerName);
      Email.notifyOnBrokerReplacement(drkafkaConfig.getNotificationEmails(),
          clusterName, brokerName);
    }
    return true;
  }

  public void enableMaintenanceMode() {
    maintenanceMode.set(true);
    LOG.info("Enabled maintenace mode for:" + clusterConfig.getClusterName());
    Email.notifyOnMaintenanceMode(drkafkaConfig.getNotificationEmails(), 
        clusterConfig.getClusterName(), maintenanceMode.get());
  }
  
  public void disableMaintenanceMode() {
    maintenanceMode.set(false);
    LOG.info("Disabled maintenace mode for:" + clusterConfig.getClusterName());
    Email.notifyOnMaintenanceMode(drkafkaConfig.getNotificationEmails(), 
        clusterConfig.getClusterName(), maintenanceMode.get());
  }
  
  /**
   *  KafkaClusterManager periodically check the health of the cluster. If it finds
   *  an under-replicated partitions, it will perform partition reassignment. It will also
   *  do partition reassignment for workload balancing.
   *
   *  If partitions are under-replicated in the middle of work-load balancing due to
   *  broker failure, it will send out an alert. Human intervention is needed in this case.
   */
  @Override
  public void run() {
    long checkIntervalInMs = clusterConfig.getCheckIntervalInSeconds() * 1000L;
    stopped = false;
    boolean foundUrps = false;
    long firstSeenUrpsTimestamp = 0L;

    while (!stopped) {
      try {
        Thread.sleep(checkIntervalInMs);
        if (maintenanceMode.get()) {
          LOG.debug("Cluster:" + clusterConfig.getClusterName() + " is in maintenace mode");
          continue;
        }
        ZkUtils zkUtils = KafkaUtils.getZkUtils(zkUrl);

        // check if there is any broker that do not have stats.
        List<Broker> noStatsBrokers = getNoStatsBrokers();
        if (!noStatsBrokers.isEmpty()) {
          Email.alertOnNoStatsBrokers(
              drkafkaConfig.getAlertEmails(), clusterConfig.getClusterName(), noStatsBrokers);
          continue;
        }

        Seq<String> topicsSeq = zkUtils.getAllTopics();
        List<String> topics = scala.collection.JavaConverters.seqAsJavaList(topicsSeq);
        scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>>
            partitionAssignments = zkUtils.getPartitionAssignmentForTopics(topicsSeq);

        Map<String, Integer> replicationFactors = new HashMap<>();
        Map<String, Integer> partitionCounts = new HashMap<>();
        topics.stream().forEach(topic -> {
          int partitionCount = partitionAssignments.get(topic).get().size();
          int factor = partitionAssignments.get(topic).get().head()._2().size();
          partitionCounts.put(topic, partitionCount);
          replicationFactors.put(topic, factor);
        });

        underReplicatedPartitions = getUnderReplicatedPartitions(zkUrl, securityProtocol, consumerConfigs,
            topics, partitionAssignments, replicationFactors, partitionCounts);
        LOG.info("Under-replicated partitions: {}", underReplicatedPartitions.size());

        for (PartitionInfo partitionInfo : underReplicatedPartitions) {
          LOG.info("under-replicated : {}", partitionInfo);
        }

        kafkaCluster.clearResourceAllocationCounters();
        if (underReplicatedPartitions.size() > 0) {
          // handle under-replicated partitions
          if (!foundUrps) {
            foundUrps = true;
            firstSeenUrpsTimestamp = System.currentTimeMillis();
          } else {
            // send out an alert if the cluster has been under-replicated for a while
            long underReplicatedTimeMills = System.currentTimeMillis() - firstSeenUrpsTimestamp;
            if (underReplicatedTimeMills > clusterConfig.getUnderReplicatedAlertTimeInMs()) {

              Email.alertOnProlongedUnderReplicatedPartitions(drkafkaConfig.getAlertEmails(),
                  clusterConfig.getClusterName(),
                  clusterConfig.getUnderReplicatedAlertTimeInSeconds(),
                  underReplicatedPartitions);
            }
          }
          LOG.info("Under-replicated partitions in cluster {} : {}",
              clusterConfig.getClusterName(), underReplicatedPartitions.size());

          handleUnderReplicatedPartitions(underReplicatedPartitions, replicationFactors);
        } else {
          foundUrps = false;
          firstSeenUrpsTimestamp = Long.MAX_VALUE;
          if (clusterConfig.enabledWorloadBalancing()) {
            preferredLeaders.clear();
            reassignmentMap.clear();
            balanceWorkload();
          }
        }
        if (clusterConfig.enabledDeadbrokerReplacement()) {
          // replace the brokers that do not have kafkastats update for a while
          checkAndReplaceDeadBrokers();
        }
      } catch (Exception e) {
        LOG.error("Unexpected failure in cluster manager for {}: ", zkUrl, e);
      }
    }
  }

  public boolean isMaintenanceModeEnabled() {
    return maintenanceMode.get();
  }
}
