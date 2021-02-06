package com.pinterest.doctork.tools;


import com.pinterest.doctork.KafkaClusterManager;
import com.pinterest.doctork.util.KafkaUtils;

import kafka.utils.ZkUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    List<PartitionInfo> urps = KafkaClusterManager.getUnderReplicatedPartitions(
        zookeeper, SecurityProtocol.PLAINTEXT, null, topics, partitionAssignments, replicationFactors, partitionCounts);

    for (PartitionInfo partitionInfo : urps) {
      LOG.info("under-replicated : {}", partitionInfo);
    }
  }
}
