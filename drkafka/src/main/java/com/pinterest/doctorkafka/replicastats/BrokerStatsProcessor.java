package com.pinterest.doctorkafka.replicastats;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.DoctorKafkaMetrics;
import com.pinterest.doctorkafka.util.OpenTsdbMetricConverter;
import com.pinterest.doctorkafka.util.OperatorUtil;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

/**
 *  Daemon thread for reading data from brokerStats topic.
 */
public class BrokerStatsProcessor implements Runnable {

  private static final Logger LOG = LogManager.getLogger(BrokerStatsProcessor.class);
  private static final long BROKER_STATS_POLL_INTERVAL_MS = 200L;
  private static final String BROKERSTATS_CONSUMER_GROUP =
      "operator_brokerstats_group_" + OperatorUtil.getHostname();
  protected Thread thread;
  private boolean stopped = true;
  private String zkUrl;
  private String topic;
  private SecurityProtocol securityProtocol;
  private Map<String, String> consumerConfigs;

  public BrokerStatsProcessor(String zkUrl, SecurityProtocol securityProtocol,
      String topic, Map<String, String> consumerConfigs) {
    this.zkUrl = zkUrl;
    this.topic = topic;
    this.securityProtocol = securityProtocol;
    this.consumerConfigs = consumerConfigs;
  }


  public void start() {
    this.thread = new Thread(this);
    this.thread.start();
  }

  public void stop() {
    this.stopped = true;
  }

  /**
   * Method for retrieving the data from kafka.
   */
  @Override
  public void run() {
    thread.setUncaughtExceptionHandler(new BrokerStatsReaderExceptionHandler());
    this.stopped = false;
    try {
      Properties properties = OperatorUtil.createKafkaConsumerProperties(
          zkUrl, BROKERSTATS_CONSUMER_GROUP, securityProtocol, consumerConfigs);
      @SuppressWarnings("resource")
      KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);

      consumer.subscribe(Arrays.asList(topic));
      while (!stopped) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(BROKER_STATS_POLL_INTERVAL_MS);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          try {

            BrokerStats brokerStats = OperatorUtil.deserializeBrokerStats(record);
            if (brokerStats == null || brokerStats.getName() == null) {
              // ignore the messages that the operator fails to deserialize
              continue;
            }

            ReplicaStatsManager.update(brokerStats);
            OpenTsdbMetricConverter.incr(DoctorKafkaMetrics.BROKERSTATS_MESSAGES, 1,
                "zkUrl= " + brokerStats.getZkUrl());
          } catch (Exception e) {
            LOG.debug("Fail to decode an message", e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Caught exception in getting broker stats, exiting. ", e);
      System.exit(-1);
    }
  }

  class BrokerStatsReaderExceptionHandler implements Thread.UncaughtExceptionHandler {

    public void uncaughtException(Thread t, Throwable e) {
      LOG.error("Unexpected exception : ", e);
      System.exit(1);
    }
  }
}
