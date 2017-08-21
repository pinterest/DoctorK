package com.pinterest.doctorkafka.stats;

import com.pinterest.doctorkafka.BrokerStats;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerStatsReporter implements Runnable {

  private static final Logger LOG = LogManager.getLogger(BrokerStatsReporter.class);
  private static final int INITIAL_DELAY = 0;

  public static ScheduledExecutorService statsReprotExecutor;

  static {
    statsReprotExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("StatsReporter").build());
  }

  private String brokerHost;
  private String jmxPort;
  private String kafkaConfigPath;
  private String statsTopic;
  private String zkUrl;
  private long pollingIntervalInSeconds;

  public BrokerStatsReporter(String kafkaConfigPath, String host, String jmxPort,
                             String statsTopic, String zkUrl, long pollingIntervalInSeconds) {
    this.brokerHost = host;
    this.jmxPort = jmxPort;
    this.kafkaConfigPath = kafkaConfigPath;
    this.statsTopic = statsTopic;
    this.zkUrl = zkUrl;
    this.pollingIntervalInSeconds = pollingIntervalInSeconds;
  }


  public void start() {
    LOG.info("Starting broker stats reporter.....");
    statsReprotExecutor.scheduleAtFixedRate(
        this, INITIAL_DELAY, pollingIntervalInSeconds, TimeUnit.SECONDS);
  }

  public void stop() throws Exception {
    statsReprotExecutor.shutdown();
  }

  @Override
  public void run() {
    BrokerStatsRetriever brokerStatsRetriever = new BrokerStatsRetriever(kafkaConfigPath);
    try {
      BrokerStats stats = brokerStatsRetriever.retrieveBrokerStats(brokerHost, jmxPort);
      KafkaAvroPublisher avroPublisher = new KafkaAvroPublisher(statsTopic, zkUrl);
      avroPublisher.publish(stats);
      LOG.info("publised to kafka : {}", stats);
    } catch (Exception e) {
      LOG.error("Faield to report stats", e);
    }
  }
}
