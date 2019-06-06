package com.pinterest.doctorkafka.modules.monitor.cluster.kafka;

import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.OperatorUtil;

import kafka.utils.ZkUtils;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class URPMonitor extends KafkaMonitor {

  private static final Logger LOG = LogManager.getLogger(URPMonitor.class);


  private static final String CONFIG_CONSUMER_CONFIG_KEY = "consumer";
  private static final String CONFIG_SECURITY_PROTOCOL_KEY = CONFIG_CONSUMER_CONFIG_KEY + ".security.protocol";

  private SecurityProtocol configSecurityProtocol = SecurityProtocol.PLAINTEXT;
  private Map<String, String> configConsumerConfigs = new HashMap<>();

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);

    Configuration tmpConsumerConfig = config.subset(CONFIG_CONSUMER_CONFIG_KEY);
    Iterator<String> keys = tmpConsumerConfig.getKeys();
    keys.forEachRemaining(k -> configConsumerConfigs.put(k, tmpConsumerConfig.getString(k)));

    configSecurityProtocol = config.containsKey(CONFIG_SECURITY_PROTOCOL_KEY) ?
                             Enum.valueOf(SecurityProtocol.class, config.getString(CONFIG_SECURITY_PROTOCOL_KEY)) :
                             configSecurityProtocol;
  }

  public KafkaState observe(KafkaContext ctx, KafkaState state){
    ZkUtils zkUtils = ctx.getZkUtils();
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

    List<PartitionInfo> underReplicatedPartitions = getUnderReplicatedPartitions(
        ctx.getZkUrl(), configSecurityProtocol, configConsumerConfigs,
        topics, partitionAssignments, replicationFactors, partitionCounts
    );
    List<PartitionInfo> filteredURPs = filterOutInReassignmentUrps(underReplicatedPartitions, replicationFactors);
    LOG.info("Under-replicated partitions: {}", filteredURPs.size());
    state.setUnderReplicatedPartitions(filteredURPs);

    for (PartitionInfo partitionInfo : filteredURPs) {
      LOG.info("under-replicated : {}", partitionInfo);
    }
    return state;
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
    List<PartitionInfo> underReplicated = new ArrayList<>();
    KafkaConsumer
        kafkaConsumer = KafkaUtils.getKafkaConsumer(zkUrl, securityProtocol, consumerConfigs);
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

}
