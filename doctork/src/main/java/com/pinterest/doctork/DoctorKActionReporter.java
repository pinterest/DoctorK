package com.pinterest.doctork;

import com.pinterest.doctork.util.OperatorUtil;

import java.util.Map;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.concurrent.Future;

public class DoctorKActionReporter {

  private static final Logger LOG = LogManager.getLogger(DoctorKActionReporter.class);
  private static final int MAX_RETRIES = 5;
  private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();
  private static final SpecificDatumWriter<OperatorAction> avroWriter
      = new SpecificDatumWriter<>(OperatorAction.SCHEMA$);

  private String topic;
  private final Producer<byte[], byte[]> kafkaProducer;

  public DoctorKActionReporter(String zkUrl, SecurityProtocol securityProtocol,
                               String topic, Map<String, String> producerConfigs) {
    this.topic = topic;
    String bootstrapBrokers = OperatorUtil.getBrokers(zkUrl, securityProtocol);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1638400);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

    for (Map.Entry<String, String> entry : producerConfigs.entrySet()) {
      props.put(entry.getKey(), entry.getValue());
    }
    this.kafkaProducer = new KafkaProducer<>(props);
  }

  public synchronized void sendMessage(String clusterName, String message) {
    int numRetries = 0;
    while (numRetries < MAX_RETRIES) {
      try {
        long timestamp = System.currentTimeMillis();
        OperatorAction operatorAction = new OperatorAction(timestamp, clusterName, message);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(stream, null);
        avroWriter.write(operatorAction, binaryEncoder);
        binaryEncoder.flush();
        IOUtils.closeQuietly(stream);

        String key = Long.toString(System.currentTimeMillis());
        ProducerRecord<byte[], byte[]>  producerRecord = 
            new ProducerRecord<>(topic, key.getBytes(), stream.toByteArray());
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        future.get();
        LOG.info("Send an message {} to action report : ", message);
        break;
      } catch (Exception e) {
        LOG.error("Failed to publish report message {}: {}", clusterName, message, e);
        numRetries++;
      }
    }
  }
}
