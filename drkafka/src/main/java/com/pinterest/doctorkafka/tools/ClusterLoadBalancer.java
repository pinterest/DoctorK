package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/**
 * Generate the partition reassignment plan for balancing the workload
 */
public class ClusterLoadBalancer {

  private static final Logger LOG = LogManager.getLogger(ClusterLoadBalancer.class);
  private static final String CONFIG = "config";
  private static final String BROKERSTATS_ZOOKEEPER = "brokerstatszk";
  private static final String BROKERSTATS_TOPIC = "brokerstatstopic";
  private static final String CLUSTER_ZOOKEEPER = "clusterzk";
  private static final String SECONDS = "seconds";
  private static final String ONLY_ONE = "onlyone";

  private static final Options options = new Options();

  /**
   *  Usage:  ClusterLoadBalancer  \
   *             -brokerstatszk    zookeeper001:2181/cluster1  \
   *             -brokerstatstopic brokerstats                 \
   *             -clusterzk  zookeeper001:2181,...,zookeeper007:2181/cluster2 \
   *             -brokerids  2048,2051  -seconds  43200
   *             -onlyone
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option brokerStatsZookeeper =
        new Option(BROKERSTATS_ZOOKEEPER, true, "zookeeper for brokerstats topic");
    Option config = new Option(CONFIG, true, "doctorkafka config");
    Option brokerStatsTopic = new Option(BROKERSTATS_TOPIC, true, "topic for brokerstats");
    Option clusterZookeeper = new Option(CLUSTER_ZOOKEEPER, true, "cluster zookeeper");
    Option seconds = new Option(SECONDS, true, "examined time window in seconds");
    Option onlyOne = new Option(ONLY_ONE, false, "only balance broker that has the heaviest load");
    options.addOption(config).addOption(brokerStatsZookeeper).addOption(brokerStatsTopic)
        .addOption(clusterZookeeper).addOption(seconds).addOption(onlyOne);

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
    formatter.printHelp("ClusterLoadBalancer", options);
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String configFilePath = commandLine.getOptionValue(CONFIG);
    String brokerStatsZk = commandLine.getOptionValue(BROKERSTATS_ZOOKEEPER);
    String brokerStatsTopic = commandLine.getOptionValue(BROKERSTATS_TOPIC);
    String clusterZk = commandLine.getOptionValue(CLUSTER_ZOOKEEPER);
    long seconds = Long.parseLong(commandLine.getOptionValue(SECONDS));
    boolean onlyOne = commandLine.hasOption(ONLY_ONE);

    ReplicaStatsManager.config = new DoctorKafkaConfig(configFilePath);
    ReplicaStatsManager.readPastReplicaStats(brokerStatsZk, SecurityProtocol.PLAINTEXT, 
        brokerStatsTopic, seconds);
    Set<String> zkUrls = ReplicaStatsManager.config.getClusterZkUrls();
    if (!zkUrls.contains(clusterZk)) {
      LOG.error("Failed to find zkurl {} in configuration", clusterZk);
      return;
    }

    DoctorKafkaClusterConfig clusterConf =
        ReplicaStatsManager.config.getClusterConfigByZkUrl(clusterZk);
    KafkaCluster kafkaCluster = ReplicaStatsManager.clusters.get(clusterZk);
    KafkaClusterManager clusterManager = new KafkaClusterManager(
        clusterZk, kafkaCluster, clusterConf, ReplicaStatsManager.config, null, null);

    List<KafkaBroker> highTrafficBrokers = clusterManager.getHighTrafficBroker();
    if (onlyOne && highTrafficBrokers.size() > 0) {
      KafkaBroker broker = highTrafficBrokers.get(0);
      highTrafficBrokers.clear();
      highTrafficBrokers.add(broker);
    }

    String assignPlan = clusterManager.getWorkloadBalancingPlanInJson(highTrafficBrokers);
    LOG.info("Reassignment Plan : {}", assignPlan);
  }
}
