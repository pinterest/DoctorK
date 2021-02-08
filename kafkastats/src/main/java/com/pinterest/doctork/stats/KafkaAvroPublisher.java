package com.pinterest.doctork.stats;

import com.pinterest.doctork.util.OpenTsdbMetricConverter;
import com.pinterest.doctork.util.OperatorUtil;
import com.pinterest.doctork.BrokerStats;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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

  public KafkaAvroPublisher(String zkUrl, String topic, String statsProducerPropertiesFile) {
    this.destTopic = topic;
    Properties statsProducerProperties = new Properties();
    Map<String, Object> keyValueMap = new HashMap<>();
    try {
      if (statsProducerPropertiesFile != null) {
        statsProducerProperties.load(new FileInputStream(statsProducerPropertiesFile));
        for (String propertyName : statsProducerProperties.stringPropertyNames()) {
          keyValueMap.put(propertyName, statsProducerProperties.get(propertyName));
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to load configuration file {}", statsProducerPropertiesFile, e);
    }
    // set the security protocol based on
    SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
    if (keyValueMap.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
      String secStr = keyValueMap.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).toString();
      securityProtocol = Enum.valueOf(SecurityProtocol.class, secStr);
    }
    Properties producerProperties = OperatorUtil.createKafkaProducerProperties(zkUrl, securityProtocol);
    for (Map.Entry<String, Object> entry: keyValueMap.entrySet()) {
      producerProperties.put(entry.getKey(), entry.getValue());
    }
    this.kafkaProducer = new KafkaProducer<>(producerProperties);
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
          new ProducerRecord<>(destTopic, partition, key.getBytes(), stream.toByteArray()));
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
