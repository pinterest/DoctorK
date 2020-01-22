package com.pinterest.doctorkafka.plugins.task.cluster.kafka;

import com.pinterest.doctorkafka.OperatorAction;
import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.task.Task;
import com.pinterest.doctorkafka.plugins.task.TaskHandler;
import com.pinterest.doctorkafka.plugins.task.TaskUtils;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * This action logs operational tasks to a Kafka topic
 *
 * <pre>
 * Configuration:
 * [required]
 * config.topic=< topic to log tasks to>
 * config.zkurl=< zkurl of the kafka cluster>
 * [optional]
 * config.producer.< any kafka-specific configs needed >=< value >
 * config.producer.security.protocol=< security protocol for the kafka consumer>
 *
 * Input task Format:
 * {
 *   subject: str,
 *   message: str
 * }
 * </pre>
 */

public class KafkaReportOperationTaskHandler extends TaskHandler {
  
  private static final Logger LOG = LogManager.getLogger(KafkaReportOperationTaskHandler.class);
  private static final int MAX_RETRIES = 5;
  private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();
  private static final SpecificDatumWriter<OperatorAction> avroWriter
      = new SpecificDatumWriter<>(OperatorAction.SCHEMA$);

  private final static String CONFIG_TOPIC_KEY = "topic";
  private final static String CONFIG_ZKURL_KEY = "zkurl";
  private final static String CONFIG_PRODUCER_CONFIG_KEY = "producer_config";
  private final static String CONFIG_SECURITY_PROTOCOL_KEY = "security.protocol";

  private final static SecurityProtocol DEFAULT_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;

  private static String configTopic;

  private static boolean configured = false;
  private static Producer<byte[], byte[]> kafkaProducer;

  @Override
  public synchronized void configure(ImmutableConfiguration config) throws
                                                                   PluginConfigurationException {
    if (!configured) {
      if(!config.containsKey(CONFIG_TOPIC_KEY)){
        throw new PluginConfigurationException("Missing config " + CONFIG_TOPIC_KEY + " in plugin " + KafkaReportOperationTaskHandler.class);
      }
      configTopic = config.getString(CONFIG_TOPIC_KEY);
      kafkaProducer = createReportKafkaProducerFromConfig(config);
      configured = true;
    }
  }

  @Override
  public Collection<Task> execute(Task task) throws Exception {
    if(task.containsAttribute(TaskUtils.TASK_SUBJECT_KEY) && task.containsAttribute(TaskUtils.TASK_MESSAGE_KEY)){
      String subject = (String) task.getAttribute(TaskUtils.TASK_SUBJECT_KEY);
      String message = (String) task.getAttribute(TaskUtils.TASK_MESSAGE_KEY);
      report(subject, message);
    }
    return null;
  }

  public synchronized void report(String subject, String message) throws Exception {
    int numRetries = 0;
    while (numRetries < MAX_RETRIES) {
      try {
        long timestamp = System.currentTimeMillis();
        OperatorAction operatorAction = new OperatorAction(timestamp, subject, message);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(stream, null);
        avroWriter.write(operatorAction, binaryEncoder);
        binaryEncoder.flush();
        IOUtils.closeQuietly(stream);

        String key = Long.toString(System.currentTimeMillis());
        ProducerRecord<byte[], byte[]> producerRecord =
            new ProducerRecord<>(configTopic, key.getBytes(), stream.toByteArray());
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        future.get();
        LOG.info("Send an message {} to action report : ", message);
        return;
      } catch (Exception e) {
        LOG.error("Failed to publish report message {}: {}", subject, message, e);
        numRetries++;
      }
    }
    LOG.error("Failed to report " + subject + " action: " + message);
  }

  protected KafkaProducer<byte[], byte[]> createReportKafkaProducerFromConfig(ImmutableConfiguration config)
      throws PluginConfigurationException {

    if(!config.containsKey(CONFIG_ZKURL_KEY)){
      throw new PluginConfigurationException("Missing config " + CONFIG_ZKURL_KEY + " in plugin " + KafkaReportOperationTaskHandler.class);
    }
    String zkUrl = config.getString(CONFIG_ZKURL_KEY);

    Properties producerConfig = new Properties();
    if(config.containsKey(CONFIG_PRODUCER_CONFIG_KEY)){
      try {
        producerConfig.load(new StringReader(config.getString(CONFIG_PRODUCER_CONFIG_KEY)));
      } catch (IOException e){
        throw new PluginConfigurationException("Failed to parse properties from key: " + CONFIG_PRODUCER_CONFIG_KEY + " in plugin " + KafkaReportOperationTaskHandler.class);
      }
    }

    SecurityProtocol securityProtocol = DEFAULT_SECURITY_PROTOCOL;
    if(producerConfig.containsKey(CONFIG_SECURITY_PROTOCOL_KEY)){
      securityProtocol = SecurityProtocol.valueOf(producerConfig.getProperty(CONFIG_SECURITY_PROTOCOL_KEY));
    }

    String bootstrapBrokers = OperatorUtil.getBrokers(zkUrl, securityProtocol);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1638400);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaUtils.BYTE_ARRAY_SERIALIZER);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaUtils.BYTE_ARRAY_SERIALIZER);

    props.putAll(producerConfig);
    return new KafkaProducer<>(props);
  }
}
