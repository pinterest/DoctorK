package com.pinterest.doctorkafka.modules.action;

import com.pinterest.doctorkafka.OperatorAction;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.event.Event;
import com.pinterest.doctorkafka.modules.event.EventUtils;
import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * This action logs operational events to a Kafka topic
 *
 * Configuration:
 * [required]
 * config.topic=< topic to log events to>
 * config.zkurl=< zkurl of the kafka cluster>
 * [optional]
 * config.producer.< any kafka-specific configs needed >=< value >
 * config.producer.security.protocol=< security protocol for the kafka consumer>
 *
 * Input Event Format:
 * {
 *   subject: str,
 *   message: str
 * }
 *
 */

public class KafkaReportOperationAction extends Action {
  private static final Logger LOG = LogManager.getLogger(KafkaReportOperationAction.class);
  private static final int MAX_RETRIES = 5;
  private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();
  private static final SpecificDatumWriter<OperatorAction> avroWriter
      = new SpecificDatumWriter<>(OperatorAction.SCHEMA$);

  private final static String CONFIG_TOPIC_KEY = "topic";
  private final static String CONFIG_ZKURL_KEY = "zkurl";
  private final static String CONFIG_PRODUCER_CONFIG_KEY = "producer";
  private final static String CONFIG_SECURITY_PROTOCOL_KEY = CONFIG_PRODUCER_CONFIG_KEY + ".security.protocol";

  private final static SecurityProtocol DEFAULT_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;

  private static String configTopic;

  private static boolean configured = false;
  private static Producer<byte[], byte[]> kafkaProducer;

  @Override
  public synchronized void configure(AbstractConfiguration config) throws
                                                                   ModuleConfigurationException {
    super.configure(config);
    if (!configured) {
      if(!config.containsKey(CONFIG_TOPIC_KEY)){
        throw new ModuleConfigurationException("Missing config " + CONFIG_TOPIC_KEY + " in plugin " + KafkaReportOperationAction.class);
      }
      configTopic = config.getString(CONFIG_TOPIC_KEY);
      kafkaProducer = createReportKafkaProducerFromConfig(config);
      configured = true;
    }
  }

  @Override
  public Collection<Event> execute(Event event) throws Exception {
    if(event.containsAttribute(EventUtils.EVENT_SUBJECT_KEY) && event.containsAttribute(EventUtils.EVENT_MESSAGE_KEY)){
      String subject = (String) event.getAttribute(EventUtils.EVENT_SUBJECT_KEY);
      String message = (String) event.getAttribute(EventUtils.EVENT_MESSAGE_KEY);
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

  protected KafkaProducer<byte[], byte[]> createReportKafkaProducerFromConfig(AbstractConfiguration config)
      throws ModuleConfigurationException {

    if(!config.containsKey(CONFIG_ZKURL_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_ZKURL_KEY + " in plugin " + KafkaReportOperationAction.class);
    }
    String zkUrl = config.getString(CONFIG_ZKURL_KEY);
    SecurityProtocol securityProtocol = config.containsKey(CONFIG_SECURITY_PROTOCOL_KEY) ?
                                        Enum.valueOf(SecurityProtocol.class, config.getString(CONFIG_SECURITY_PROTOCOL_KEY)) :
                                        DEFAULT_SECURITY_PROTOCOL;

    String bootstrapBrokers = OperatorUtil.getBrokers(zkUrl, securityProtocol);
    Configuration tmpProducerConfig = config.subset(CONFIG_PRODUCER_CONFIG_KEY);
    Iterator<String> keys = tmpProducerConfig.getKeys();

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1638400);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

    keys.forEachRemaining(k -> props.put(k, tmpProducerConfig.getString(k)));
    return new KafkaProducer<>(props);
  }
}
