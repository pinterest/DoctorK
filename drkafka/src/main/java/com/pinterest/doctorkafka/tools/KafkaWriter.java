package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import org.apache.kafka.common.security.auth.SecurityProtocol;


public class KafkaWriter {

  private static final String ZOOKEEPER = "zookeeper";
  private static final String TOPIC = "topic";
  private static final String NUM_MESSAGES = "num_messages";
  private static final Options options = new Options();

  /**
   *  Usage:  KafkaWriter  \
   *             --zookeeper datazk001:2181/testk10 --topic kafka_test    \
   *             --num_messages 100
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option zookeeper = new Option(ZOOKEEPER, true, "zookeeper connection string");
    Option topic = new Option(TOPIC, true, "topic that KafkaWriter writes to");
    Option num_messages = new Option(NUM_MESSAGES, true, "num of messags that writes to kafka");
    options.addOption(zookeeper).addOption(topic).addOption(num_messages);

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
    String zkUrl = commandLine.getOptionValue(ZOOKEEPER);
    String topic = commandLine.getOptionValue(TOPIC);
    int numMessages = Integer.parseInt(commandLine.getOptionValue(NUM_MESSAGES));

    Random random = new Random();
    Properties props = OperatorUtil.createKafkaProducerProperties(zkUrl, SecurityProtocol.PLAINTEXT);
    KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(props);

    byte[] key = new byte[16];
    byte[] data = new byte[1024];
    for (int i = 0; i < numMessages; i++) {
      for (int j = 0; j < data.length; j++) {
        data[j] = (byte)random.nextInt();
      }
      ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
          topic, 0, System.currentTimeMillis(), key, data);
      Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
      future.get();
      if (i % 100 == 0) {
        System.out.println("Have wrote " + i + " messages to kafka");
      }
    }
    kafkaProducer.close();
  }
}
