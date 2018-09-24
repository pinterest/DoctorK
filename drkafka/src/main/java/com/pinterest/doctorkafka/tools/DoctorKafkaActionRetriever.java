package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.OperatorAction;
import com.pinterest.doctorkafka.util.OperatorUtil;

import com.google.common.collect.Lists;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DoctorKafkaActionRetriever {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaActionRetriever.class);
  private static final DecoderFactory avroDecoderFactory = DecoderFactory.get();
  private static Schema operatorActionSchema = OperatorAction.getClassSchema();

  private static final String ZOOKEEPER = "zookeeper";
  private static final String TOPIC = "topic";
  private static final String NUM_MESSAGES = "num_messages";
  private static final Options options = new Options();

  /**
   *  Usage:  OperatorActionRetriever                         \
   *             -zookeeper    datazk001:2181/data07          \
   *             -topic  operator_report  -num_messages 1000
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option zookeeper = new Option(ZOOKEEPER, true, "doctorkafka action zookeeper");
    Option topic = new Option(TOPIC, true, "doctorkafka action topic");
    Option num_messages = new Option(NUM_MESSAGES, true, "num of messages to retrieve");
    options.addOption(zookeeper).addOption(topic).addOption(num_messages);

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
    formatter.printHelp("OperatorActionRetriever", options);
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String zookeeper = commandLine.getOptionValue(ZOOKEEPER);
    String topic = commandLine.getOptionValue(TOPIC);
    int num_messages = Integer.parseInt(commandLine.getOptionValue(NUM_MESSAGES));
    Properties properties = OperatorUtil.createKafkaConsumerProperties(zookeeper, "operator_action_commandline",
        SecurityProtocol.PLAINTEXT, null);

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
    TopicPartition operatorReportTopicPartition = new TopicPartition(topic, 0);
    List<TopicPartition> tps = new ArrayList<>();
    tps.add(operatorReportTopicPartition);
    consumer.assign(tps);

    Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(tps);
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);

    for (TopicPartition tp : endOffsets.keySet()) {
      long numMessages = endOffsets.get(tp) - beginOffsets.get(tp);
      LOG.info("{} : offsets [{}, {}], num messages : {}",
          tp, beginOffsets.get(tp), endOffsets.get(tp), numMessages);
      consumer.seek(tp, Math.max(beginOffsets.get(tp), endOffsets.get(tp) - num_messages));
    }

    ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
    List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
    SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    while (!records.isEmpty()) {
      for (ConsumerRecord<byte[], byte[]> record : records) {
        recordList.add(record);
      }

      for (ConsumerRecord<byte[], byte[]> record : Lists.reverse(recordList)) {
        try {
          BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(record.value(), null);
          SpecificDatumReader<OperatorAction> reader =
              new SpecificDatumReader<>(operatorActionSchema);

          OperatorAction result = new OperatorAction();
          reader.read(result, binaryDecoder);

          Date date = new Date(result.getTimestamp());
          System.out.println(date.toString() + " : " + result);
        } catch (Exception e) {
          LOG.info("Fail to decode an message", e);
        }
      }
      records = consumer.poll(100);
    }
  }
}