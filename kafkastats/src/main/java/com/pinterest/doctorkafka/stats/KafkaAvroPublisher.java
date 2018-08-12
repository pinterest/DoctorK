package com.pinterest.doctorkafka.stats;


import com.pinterest.doctorkafka.util.OpenTsdbMetricConverter;
import com.pinterest.doctorkafka.util.OperatorUtil;
import com.pinterest.doctorkafka.BrokerStats;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaAvroPublisher {

  private static final Logger LOG = LogManager.getLogger(KafkaAvroPublisher.class);
  private static final String HOSTNAME = OperatorUtil.getHostname();

  private final Producer<byte[], byte[]> kafkaProducer;
  private static final SpecificDatumWriter<BrokerStats> avroEventWriter
      = new SpecificDatumWriter<>(BrokerStats.SCHEMA$);
  private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();

  private String destTopic;

  public KafkaAvroPublisher(String topic, String zkUrl) {
    this.destTopic = topic;
    Properties props = OperatorUtil.createKafkaProducerProperties(zkUrl);
    kafkaProducer = new KafkaProducer<>(props);
  }

  public void publish(BrokerStats brokerStats) throws IOException {
    try {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(stream, null);

      avroEventWriter.write(brokerStats, binaryEncoder);
      binaryEncoder.flush();
      IOUtils.closeQuietly(stream);

      String key = brokerStats.getName() + "_" + System.currentTimeMillis();
      int numPartitions = kafkaProducer.partitionsFor(destTopic).size();
      int partition = brokerStats.getId() % numPartitions;

      Future<RecordMetadata> future = kafkaProducer.send(
          new ProducerRecord(destTopic, partition, key.getBytes(), stream.toByteArray()));
      future.get();

      OpenTsdbMetricConverter.incr("kafka.stats.collector.success", 1, "host=" + HOSTNAME);
    } catch (Exception e) {
      LOG.error("Failure in publish stats", e);
      OpenTsdbMetricConverter.incr("kafka.stats.collector.failure", 1, "host=" + HOSTNAME);
      throw new RuntimeException("Avro serialization failure", e);
    }
  }

  public void close() {
    kafkaProducer.close();
  }
}
