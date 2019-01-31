package com.pinterest.doctorkafka.stats;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.stats.HostStats;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerStatsReporter implements Runnable {

  private static final Logger LOG = LogManager.getLogger(BrokerStatsReporter.class);
  private static final int INITIAL_DELAY = 0;

  public static ScheduledExecutorService statsReportExecutor;

  static {
    statsReportExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("StatsReporter").build());
  }

  private String brokerHost;
  private String jmxPort;
  private String kafkaConfigPath;
  private KafkaAvroPublisher avroPublisher;
  private long pollingIntervalInSeconds;
  private String primaryNetworkInterfaceName;
  private HostStats hostStats;

  public BrokerStatsReporter(String kafkaConfigPath, String host, String jmxPort,
                             KafkaAvroPublisher avroPublisher, long pollingIntervalInSeconds, String primaryNetworkInterfaceName) {
    this.brokerHost = host;
    this.jmxPort = jmxPort;
    this.kafkaConfigPath = kafkaConfigPath;
    this.avroPublisher = avroPublisher;
    this.pollingIntervalInSeconds = pollingIntervalInSeconds;
    this.primaryNetworkInterfaceName = primaryNetworkInterfaceName;
    this.hostStats = new HostStats();
  }


  public void start() {
    LOG.info("Starting broker stats reporter.....");
    statsReportExecutor.scheduleAtFixedRate(
        this, INITIAL_DELAY, pollingIntervalInSeconds, TimeUnit.SECONDS);
  }

  public void stop() throws Exception {
    statsReportExecutor.shutdown();
  }

  @Override
  public void run() {
    BrokerStatsRetriever brokerStatsRetriever = new BrokerStatsRetriever(kafkaConfigPath, primaryNetworkInterfaceName, hostStats);
    try {
      BrokerStats stats = brokerStatsRetriever.retrieveBrokerStats(brokerHost, jmxPort);
      avroPublisher.publish(stats);
      LOG.info("published to kafka : {}", stats);
    } catch (Exception e) {
      LOG.error("Failed to report stats", e);
    }
  }
}
