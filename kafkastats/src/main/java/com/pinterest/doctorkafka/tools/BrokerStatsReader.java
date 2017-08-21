package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class BrokerStatsReader {

  private static final Logger LOG = LogManager.getLogger(BrokerStatsReader.class);
  private static final String ZOOKEEPER = "zookeeper";
  private static final String STATS_TOPIC = "topic";
  private static final DecoderFactory avroDecoderFactory = DecoderFactory.get();

  private static final Options options = new Options();

  private static CommandLine parseCommandLine(String[] args) {
    if (args.length < 4) {
      printUsageAndExit();
    }

    Option zookeeper = new Option(ZOOKEEPER, true, "zookeeper connection string");
    Option statsTopic = new Option(STATS_TOPIC, true, "kafka topic for broker stats");
    options.addOption(zookeeper).addOption(statsTopic);

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
    formatter.printHelp("BrokerStatsReader", options);
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String zkUrl = commandLine.getOptionValue(ZOOKEEPER);
    String statsTopic = commandLine.getOptionValue(STATS_TOPIC);

    String boostrapBrokers = OperatorUtil.getBrokers(zkUrl);
    Properties props = new Properties();
    props.put("bootstrap.servers", boostrapBrokers);
    props.put("group.id", "broker_statsreader_group");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    Schema schema = BrokerStats.getClassSchema();
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(statsTopic));
    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
      for (ConsumerRecord<byte[], byte[]> record : records) {
        System.out.printf("offset = %d, key.size = %d, value.size = %s%n",
            record.offset(), record.key().length, record.value().length);
        try {
          BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(record.value(), null);
          SpecificDatumReader<BrokerStats> reader = new SpecificDatumReader<>(schema);
          BrokerStats result = new BrokerStats();
          reader.read(result, binaryDecoder);
          System.out.println(result);
        } catch (Exception e) {
          LOG.error("Fail to decode an message", e);
        }
      }
    }
  }
}
