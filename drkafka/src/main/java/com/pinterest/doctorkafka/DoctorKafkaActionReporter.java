package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.util.OperatorUtil;
import com.pinterest.doctorkafka.util.KafkaUtils;

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
import java.util.Properties;
import java.util.concurrent.Future;

public class DoctorKafkaActionReporter {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaActionReporter.class);
  private static final int MAX_RETRIES = 5;
  private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();
  private static final SpecificDatumWriter<OperatorAction> avroWriter
      = new SpecificDatumWriter<>(OperatorAction.SCHEMA$);

  private String topic;
  private final Producer<byte[], byte[]> kafkaProducer;

  public DoctorKafkaActionReporter(String topic, String zkUrl) {
    this.topic = topic;
    String bootstrapBrokers = OperatorUtil.getBrokers(zkUrl);
    Properties props = new Properties();
    props.put(KafkaUtils.BOOTSTRAP_SERVERS, bootstrapBrokers);
    props.put("acks", "1");
    props.put("retries", 0);
    props.put("batch.size", 1638400);
    props.put("buffer.memory", 33554432);
    props.put("compression.type", "gzip");
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<>(props);
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

        String key = "" + System.currentTimeMillis();
        ProducerRecord  producerRecord =
            new ProducerRecord(topic, key.getBytes(), stream.toByteArray());
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
