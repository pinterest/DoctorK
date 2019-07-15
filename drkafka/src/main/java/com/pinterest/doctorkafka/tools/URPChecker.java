package com.pinterest.doctorkafka.tools;


import com.pinterest.doctorkafka.util.KafkaUtils;

import kafka.utils.ZkUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class URPChecker {

  private static final Logger LOG = LogManager.getLogger(URPChecker.class);

  private static final String ZOOKEEPER = "zookeeper";
  private static final Options options = new Options();

  /**
   *  Usage:  URPChecker  \
   *             -brokerstatszk    datazk001:2181/data07    \
   *             -brokerstatstopic brokerstats              \
   *             -clusterzk  m10nzk001:2181,...,m10nzk007:2181/m10n07
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option zookeeper = new Option(ZOOKEEPER, true, "cluster zookeeper");
    options.addOption(zookeeper);

    if (args.length < 2) {
      printUsageAndExit();
    }

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException | NumberFormatException e) {
      printUsageAndExit();
    }
    return cmd;
  }

  private static void printUsageAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("ClusterLoadBalancer", options);
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String zookeeper = commandLine.getOptionValue(ZOOKEEPER);

    ZkUtils zkUtils = KafkaUtils.getZkUtils(zookeeper);
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

    List<PartitionInfo> urps = getUnderReplicatedPartitions(
        zookeeper, SecurityProtocol.PLAINTEXT, null, topics, partitionAssignments, replicationFactors, partitionCounts);

    for (PartitionInfo partitionInfo : urps) {
      LOG.info("under-replicated : {}", partitionInfo);
    }
  }


  /**
   * Call the kafka api to get the list of under-replicated partitions.
   * When a topic partition loses all of its replicas, it will not have a leader broker.
   * We need to handle this special case in detecting under replicated topic partitions.
   */
  protected static List<PartitionInfo> getUnderReplicatedPartitions(
      String zkUrl, SecurityProtocol securityProtocol, Properties consumerConfigs,
      List<String> topics,
      scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignments,
      Map<String, Integer> replicationFactors,
      Map<String, Integer> partitionCounts) {
    List<PartitionInfo> underReplicated = new ArrayList<>();
    KafkaConsumer<byte[], byte[]>
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
}
