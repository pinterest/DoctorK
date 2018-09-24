package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.DoctorKafkaActionReporter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.security.auth.SecurityProtocol;

public class DoctorKafkaActionWriter {

  private static final String ZOOKEEPER = "zookeeper";
  private static final String TOPIC = "topic";
  private static final String MESSAGE = "message";
  private static final Options options = new Options();

  /**
   *  Usage:  KafkaWriter  \
   *             --zookeeper zookeeper001:2181/cluster1 --topic kafka_test    \
   *             --message "this is a test message"
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option zookeeper = new Option(ZOOKEEPER, true, "zookeeper connection string");
    Option topic = new Option(TOPIC, true, "action report topic name");
    Option message = new Option(MESSAGE, true, "messags that writes to kafka");
    options.addOption(zookeeper).addOption(topic).addOption(message);

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
    formatter.printHelp("OperatorActionWriter", options);
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String zkUrl = commandLine.getOptionValue(ZOOKEEPER);
    String topic = commandLine.getOptionValue(TOPIC);
    String message = commandLine.getOptionValue(MESSAGE);

    DoctorKafkaActionReporter actionReporter =
        new DoctorKafkaActionReporter(zkUrl, SecurityProtocol.PLAINTEXT, topic, null);
    actionReporter.sendMessage("testkafka10", message);
  }
}
