package com.pinterest.doctorkafka.replicastats;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Read the broker stats in the past and reconstruct the replica stats
 */
public class PastReplicaStatsProcessor implements Runnable {

  private static final Logger LOG = LogManager.getLogger(PastReplicaStatsProcessor.class);

  private String zkUrl;
  private TopicPartition topicPartition;
  private long startOffset;
  private long endOffset;
  private Thread thread;

  public PastReplicaStatsProcessor(String zkUrl, TopicPartition topicPartition,
                               long startOffset, long endOffset) {
    this.zkUrl = zkUrl;
    this.topicPartition = topicPartition;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  public void start() {
    thread = new Thread(this);
    thread.start();
  }

  public void join() throws InterruptedException {
    this.thread.join();
  }

  public void run() {
    KafkaConsumer kafkaConsumer = null;
    try {
      String brokers = KafkaUtils.getBrokers(zkUrl);
      LOG.info("ZkUrl: {}, Brokers: {}", zkUrl, brokers);
      Properties props = new Properties();
      props.put(KafkaUtils.BOOTSTRAP_SERVERS, brokers);
      props.put(KafkaUtils.ENABLE_AUTO_COMMIT, "false");
      props.put(KafkaUtils.GROUP_ID, "kafka_operator" + topicPartition);
      props.put(KafkaUtils.KEY_DESERIALIZER,
          "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put(KafkaUtils.VALUE_DESERIALIZER,
          "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put(KafkaUtils.MAX_POLL_RECORDS, 2000);
      props.put("max.partition.fetch.bytes", 1048576 * 4);

      kafkaConsumer = new KafkaConsumer(props);
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
          ReplicaStatsManager.update(brokerStats);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in processing brokerstats", e);
    } finally {
      if (kafkaConsumer != null) {
        kafkaConsumer.close();
      }
    }
  }
}
