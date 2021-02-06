package com.pinterest.doctork.tools;

import com.pinterest.doctork.KafkaCluster;
import com.pinterest.doctork.config.DoctorKConfig;
import com.pinterest.doctork.replicastats.ReplicaStatsManager;
import com.pinterest.doctork.util.KafkaUtils;

import com.codahale.metrics.Histogram;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

public class ReplicaStatsRetriever {

  private static final Logger LOG = LogManager.getLogger(ReplicaStatsRetriever.class);

  private static final String CONFIG = "config";
  private static final String BROKERSTATS_ZOOKEEPER = "brokerstatszk";
  private static final String BROKERSTATS_TOPIC = "brokerstatstopic";
  private static final String CLUSTER_ZOOKEEPER = "clusterzk";
  private static final String SECONDS = "seconds";
  private static final Options options = new Options();

  /**
   *  Usage:  ReplicaStatsRetriever  \
   *             -brokerstatszk    datazk001:2181/data07    \
   *             -brokerstatstopic brokerstats              \
   *             -clusterzk  m10nzk001:2181,...,m10nzk007:2181/m10n07 \
   *             -seconds 43200
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option config = new Option(CONFIG, true, "operator config");
    Option brokerStatsZookeeper =
        new Option(BROKERSTATS_ZOOKEEPER, true, "zookeeper for brokerstats topic");
    Option brokerStatsTopic = new Option(BROKERSTATS_TOPIC, true, "topic for brokerstats");
    Option clusterZookeeper = new Option(CLUSTER_ZOOKEEPER, true, "cluster zookeeper");
    Option seconds = new Option(SECONDS, true, "examined time window in seconds");
    options.addOption(config).addOption(brokerStatsZookeeper).addOption(brokerStatsTopic)
        .addOption(clusterZookeeper).addOption(seconds);

    if (args.length < 6) {
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
    formatter.printHelp("ReplicaStatsRetriever", options);
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String configFilePath = commandLine.getOptionValue(CONFIG);
    String brokerStatsZk = commandLine.getOptionValue(BROKERSTATS_ZOOKEEPER);
    String brokerStatsTopic = commandLine.getOptionValue(BROKERSTATS_TOPIC);
    String clusterZk = commandLine.getOptionValue(CLUSTER_ZOOKEEPER);
    long seconds = Long.parseLong(commandLine.getOptionValue(SECONDS));

    long startTime = System.currentTimeMillis();
    ReplicaStatsManager replicaStatsManager = new ReplicaStatsManager(new DoctorKConfig(configFilePath));
    replicaStatsManager.readPastReplicaStats(brokerStatsZk, SecurityProtocol.PLAINTEXT,
        brokerStatsTopic, seconds);
    long endTime = System.currentTimeMillis();
    LOG.info("Spent time : {} seconds", (endTime - startTime) / 1000.0);

    Map<TopicPartition, Histogram> bytesInStats =
        new TreeMap<>(new KafkaUtils.TopicPartitionComparator());
    bytesInStats.putAll(replicaStatsManager.getClusters().get(clusterZk).getBytesInHistograms());
    Map<TopicPartition, Histogram> bytesOutStats =
        new TreeMap<>(new KafkaUtils.TopicPartitionComparator());
    bytesOutStats.putAll(replicaStatsManager.getClusters().get(clusterZk).getBytesOutHistograms());

    for (TopicPartition tp : bytesInStats.keySet()) {
      long maxBytesIn = bytesInStats.get(tp).getSnapshot().getMax();
      long maxBytesOut = bytesOutStats.get(tp).getSnapshot().getMax();
      System.out.println(tp + " : maxBytesIn = " + maxBytesIn + ", maxBytesOut = " + maxBytesOut);
    }

    for (KafkaCluster cluster : replicaStatsManager.getClusters().values()) {
      System.out.println("Reassignment info for " + cluster.name());
      Map<TopicPartition, Long> reassignmentTimestamps =
          cluster.getReassignmentTimestamps();
      for (TopicPartition tp : reassignmentTimestamps.keySet()) {
        System.out.println("    " + tp + " : " + reassignmentTimestamps.get(tp));
      }
    }
  }
}

