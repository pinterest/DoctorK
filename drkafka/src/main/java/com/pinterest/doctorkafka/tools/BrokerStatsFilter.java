package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.OperatorUtil;
import com.pinterest.doctorkafka.util.ReplicaStatsUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

public class BrokerStatsFilter {

  private static final Logger LOG = LogManager.getLogger(BrokerStatsFilter.class);

  private static final String CONFIG = "config";
  private static final String BROKERSTATS_ZOOKEEPER = "brokerstatszk";
  private static final String BROKERSTATS_TOPIC = "brokerstatstopic";
  private static final String BROKERNAME = "broker";
  private static final Options options = new Options();

  /**
   *  Usage:  BrokerStatsRetriever  \
   *             --brokerstatszk    datazk001:2181/data07    \
   *             --brokerstatstopic brokerstats              \
   *             --broker  kafkabroker001
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option config = new Option(CONFIG, true, "operator config");
    Option brokerStatsZookeeper =
        new Option(BROKERSTATS_ZOOKEEPER, true, "zookeeper for brokerstats topic");
    Option brokerStatsTopic = new Option(BROKERSTATS_TOPIC, true, "topic for brokerstats");
    Option broker = new Option(BROKERNAME, true, "broker name");
    options.addOption(config).addOption(brokerStatsZookeeper).addOption(brokerStatsTopic)
        .addOption(broker);

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


  public static List<BrokerStats> processOnePartition(String zkUrl, TopicPartition topicPartition,
                                                      long startOffset, long endOffset,
                                                      Set<String> brokerNames) {
    KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
    List<BrokerStats> result = new ArrayList<>();
    try {
      String brokers = KafkaUtils.getBrokers(zkUrl, SecurityProtocol.PLAINTEXT);
      LOG.info("ZkUrl: {}, Brokers: {}", zkUrl, brokers);
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_operator" + topicPartition);
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          KafkaUtils.BYTE_ARRAY_DESERIALIZER);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          KafkaUtils.BYTE_ARRAY_DESERIALIZER);
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);
      props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576 * 4);

      kafkaConsumer = new KafkaConsumer<>(props);
      Set<TopicPartition> topicPartitions = new HashSet<>();
      topicPartitions.add(topicPartition);
      kafkaConsumer.assign(topicPartitions);
      kafkaConsumer.seek(topicPartition, startOffset);

      ConsumerRecords<byte[], byte[]> records = null;
      while (kafkaConsumer.position(topicPartition) < endOffset) {
        records = kafkaConsumer.poll(100);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          BrokerStats brokerStats = OperatorUtil.deserializeBrokerStats(record);
          if (brokerStats == null || brokerStats.getName() == null) {
            continue;
          }
          if (brokerNames.contains(brokerStats.getName())) {
            result.add(brokerStats);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in processing brokerstats", e);
    } finally {
      if (kafkaConsumer != null) {
        kafkaConsumer.close();
      }
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String brokerStatsZk = commandLine.getOptionValue(BROKERSTATS_ZOOKEEPER);
    String brokerStatsTopic = commandLine.getOptionValue(BROKERSTATS_TOPIC);
    String brokerName = commandLine.getOptionValue(BROKERNAME);
    Set<String> brokerNames = new HashSet<>();
    brokerNames.add(brokerName);

    KafkaConsumer<byte[], byte[]> kafkaConsumer = KafkaUtils.getKafkaConsumer(brokerStatsZk,
        KafkaUtils.BYTE_ARRAY_DESERIALIZER,
        KafkaUtils.BYTE_ARRAY_DESERIALIZER, 1,
        SecurityProtocol.PLAINTEXT,
        null);

    long startTimestampInMillis = System.currentTimeMillis() - 86400 * 1000L;
    Map<TopicPartition, Long> offsets = ReplicaStatsUtil.getProcessingStartOffsets(
        kafkaConsumer, brokerStatsTopic, startTimestampInMillis);
    kafkaConsumer.unsubscribe();
    kafkaConsumer.assign(offsets.keySet());
    Map<TopicPartition, Long> latestOffsets = kafkaConsumer.endOffsets(offsets.keySet());
    KafkaUtils.closeConsumer(brokerStatsZk);

    Map<Long, BrokerStats> brokerStatsMap = new TreeMap<>();
    for (TopicPartition topicPartition : offsets.keySet()) {
      LOG.info("Start processing {}", topicPartition);
      long startOffset = offsets.get(topicPartition);
      long endOffset = latestOffsets.get(topicPartition);

      List<BrokerStats> statsList = processOnePartition(brokerStatsZk, topicPartition,
          startOffset, endOffset, brokerNames);
      for (BrokerStats brokerStats : statsList) {
        brokerStatsMap.put(brokerStats.getTimestamp(), brokerStats);
      }
      LOG.info("Finished processing {}, retrieved {} records", topicPartition, statsList.size());
    }

    for (Map.Entry<Long, BrokerStats> entry: brokerStatsMap.entrySet()) {
      System.out.println(entry.getKey() + " : " + entry.getValue());
    }
  }
}
