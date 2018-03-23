package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.util.ZookeeperClient;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Date;

public class DoctorKafkaZookeeperClient {

  private static final String ZOOKEEPER = "zookeeper";
  private static final String CLUSTER = "cluster";
  private static final String BROKER = "broker";
  private static final Options options = new Options();

  /**
   *  Usage:  DoctorKafkaZookeeperClient  \
   *             --zookeeper zookeeper001:2181/cluster1 --cluster cluster1  \
   *             --command terminate  --broker  broker1
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option zookeeper = new Option(ZOOKEEPER, true, "zookeeper connection string");
    Option cluster = new Option(CLUSTER, true, "cluster name");
    Option broker = new Option(BROKER, true, "broker name");
    options.addOption(zookeeper).addOption(cluster).addOption(broker);

    if (args.length < 4) {
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
    formatter.printHelp("DoctorKafkaZookeeperClient", options);
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String zkUrl = commandLine.getOptionValue(ZOOKEEPER);
    String cluster = commandLine.getOptionValue(CLUSTER);
    String broker = commandLine.getOptionValue(BROKER);
    ZookeeperClient zookeeperClient = null;

    try {
      zookeeperClient = new ZookeeperClient(zkUrl);
      zookeeperClient.recordBrokerTermination(cluster, broker);

      String replacementInfo = zookeeperClient.getBrokerReplacementInfo(cluster);
      System.out.println("Last termination: " + replacementInfo);

      long timestamp = zookeeperClient.getLastBrokerReplacementTime(cluster);
      System.out.println("Timestamp: " + timestamp + ",  " + new Date(timestamp));
    } finally {
      if (zookeeperClient != null) {
        zookeeperClient.close();
      }
    }
  }
}
