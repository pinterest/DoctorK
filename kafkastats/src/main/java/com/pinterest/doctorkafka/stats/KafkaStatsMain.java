package com.pinterest.doctorkafka.stats;

import com.pinterest.doctorkafka.util.OperatorUtil;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

/**
 * StatsCollectorMain collects all kafka operation related metrics from a kafka
 * broker, and send the metrics to a kafka topic. The following shows some
 * sample metrics that we need to collect:
 *
 * kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=ReplicaFetcherThread-0-6033,
 * \ topic=ems_processing_log,partition=4
 *
 */
public class KafkaStatsMain {

  private static final String DEFAULT_PRIMARY_INTERFACE_NAME = "eth0";
  private static final String PRIMARY_INTERFACE_NAME = "primary_network_ifacename";
  private static final Logger LOG = LogManager.getLogger(KafkaStatsMain.class);
  private static final String BROKER_NAME = "broker";
  private static final String JMX_PORT = "jmxport";
  private static final String ZOOKEEPER = "zookeeper";
  private static final String METRICS_TOPIC = "topic";
  private static final String OSTRICH_PORT = "ostrichport";
  private static final String TSD_HOSTPORT = "tsdhostport";
  private static final String UPTIME_IN_SECONDS = "uptimeinseconds";
  private static final String POLLING_INTERVAL = "pollingintervalinseconds";
  private static final String KAFKA_CONFIG = "kafka_config";
  private static final String DISABLE_EC2METADATA = "disable_ec2metadata";

  // The producer configuration for writing to the kafkastats topic
  private static final String STATS_PRODUCER_CONFIG = "producer_config";

  protected static final String hostName = OperatorUtil.getHostname();
  private static final Options options = new Options();
  private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

  /**
   * Usage: com.pinterest.kafka.KafkaStatsMain --host kafkahost --port 9999
   * --zookeeper datazk001:2181/data05 --topic kafka_metrics
   * --stats_producer_config producer_config.properties --tsdhost localhost:18321
   * --ostrichport 2051 --uptimeinseconds 43200 --pollinginterval 15
   * --kafka_config /etc/kafka/server.properties
   */
  private static CommandLine parseCommandLine(String[] args) {

    Option host = new Option(BROKER_NAME, true, "kafka broker");
    host.setRequired(false);
    Option jmxPort = new Option(JMX_PORT, true, "kafka jmx port number");
    jmxPort.setArgName("kafka jmx port number");

    Option zookeeper = new Option(ZOOKEEPER, true, "zk url for metrics topic");
    Option topic = new Option(METRICS_TOPIC, true, "kafka topic for metric messages");
    Option tsdHostPort = new Option(TSD_HOSTPORT, true, "tsd host and port, e.g. localhost:18621");
    Option ostrichPort = new Option(OSTRICH_PORT, true, "ostrich port");
    Option uptimeInSeconds = new Option(UPTIME_IN_SECONDS, true, "uptime in seconds");
    Option pollingInterval = new Option(POLLING_INTERVAL, true, "polling interval in seconds");
    Option kafkaConfig = new Option(KAFKA_CONFIG, true, "kafka server properties file path");
    Option disableEc2metadata = new Option(DISABLE_EC2METADATA, false, "Disable collecting host information via ec2metadata");
    Option statsProducerConfig = new Option(STATS_PRODUCER_CONFIG, true,
        "kafka_stats producer config");
    Option primaryNetworkInterfaceName = new Option(PRIMARY_INTERFACE_NAME, true,
        "network interface used by kafka");

    options.addOption(jmxPort).addOption(host).addOption(zookeeper).addOption(topic)
        .addOption(tsdHostPort).addOption(ostrichPort).addOption(uptimeInSeconds)
        .addOption(pollingInterval).addOption(kafkaConfig).addOption(statsProducerConfig)
        .addOption(primaryNetworkInterfaceName).addOption(disableEc2metadata);

    if (args.length < 6) {
      printUsageAndExit();
    }

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException | NumberFormatException e) {
      printUsageAndExit();
    }
    return cmd;
  }

  private static void printUsageAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("KafkaMetricsCollector", options);
    System.exit(1);
  }

  private static BrokerStatsReporter brokerStatsReporter = null;
  private static CollectorMonitor collectorMonitor = null;
  private static KafkaAvroPublisher avroPublisher = null;

  public static void main(String[] args) throws Exception {
    Runtime.getRuntime().addShutdownHook(new KafkaStatsCleanupThread());

    CommandLine commandLine = parseCommandLine(args);
    String host = commandLine.getOptionValue(BROKER_NAME);
    if (host == null || host.isEmpty()) {
      host = hostName;
    }

    String jmxPort = commandLine.getOptionValue(JMX_PORT);
    String ostrichPort = commandLine.getOptionValue(OSTRICH_PORT);
    String tsdHostPort = commandLine.getOptionValue(TSD_HOSTPORT);
    String zkUrl = commandLine.getOptionValue(ZOOKEEPER);
    String destTopic = commandLine.getOptionValue(METRICS_TOPIC);
    String kafkaConfigPath = commandLine.getOptionValue(KAFKA_CONFIG);
    long uptimeInSeconds = Long.parseLong(commandLine.getOptionValue(UPTIME_IN_SECONDS));
    long pollingInterval = Long.parseLong(commandLine.getOptionValue(POLLING_INTERVAL));
    String primaryNetworkInterfaceName = commandLine.getOptionValue(PRIMARY_INTERFACE_NAME,
        DEFAULT_PRIMARY_INTERFACE_NAME);
    boolean disableEc2metadata = commandLine.hasOption(DISABLE_EC2METADATA);

    String statsProducerPropertiesConfig = null;
    if (commandLine.hasOption(STATS_PRODUCER_CONFIG)) {
      statsProducerPropertiesConfig = commandLine.getOptionValue(STATS_PRODUCER_CONFIG);
    }

    avroPublisher = new KafkaAvroPublisher(zkUrl, destTopic, statsProducerPropertiesConfig);
    brokerStatsReporter = new BrokerStatsReporter(kafkaConfigPath, host, jmxPort, avroPublisher,
	pollingInterval, primaryNetworkInterfaceName, disableEc2metadata);
    brokerStatsReporter.start();

    collectorMonitor = new CollectorMonitor(uptimeInSeconds);
    collectorMonitor.start();
    if (tsdHostPort == null && ostrichPort == null) {
      LOG.info("OpenTSDB and Ostrich options missing, not starting Ostrich service");
    } else {
      OperatorUtil.startOstrichService("kafkastats", tsdHostPort, Integer.parseInt(ostrichPort));
    }
    shutdownLatch.await(10, TimeUnit.SECONDS);
  }

  public static class CollectorMonitor implements Runnable {

    private static final Logger LOG = LogManager.getLogger(CollectorMonitor.class);
    private static final int INITIAL_DELAY = 0;
    /**
     * The executor service for executing the monitor thread
     */
    public static ScheduledExecutorService monitorExecutor;

    static {
      monitorExecutor = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("Monitor").build());
    }

    private long restartTime;

    public CollectorMonitor(long uptimeInSeconds) {
      this.restartTime = System.currentTimeMillis() + uptimeInSeconds * 1000L;
    }

    public void start() {
      monitorExecutor.scheduleAtFixedRate(this, INITIAL_DELAY, 15, TimeUnit.SECONDS);
    }

    public void stop() throws Exception {
      monitorExecutor.shutdown();
    }

    @Override
    public void run() {
      long now = System.currentTimeMillis();
      if (now > restartTime) {
        LOG.warn("Restarting metrics collector");
        System.exit(0);
      }
    }
  }

  static class KafkaStatsCleanupThread extends Thread {
    @Override
    public void run() {
      try {
        if (brokerStatsReporter != null) {
          brokerStatsReporter.stop();
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure in brokerStatsReporter : ", t);
      }

      try {
        if (collectorMonitor != null) {
          collectorMonitor.stop();
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure in collectorMonitor : ", t);
      }

      try {
        if (avroPublisher != null) {
          avroPublisher.close();
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure in avroPublisher : ", t);
      }
      shutdownLatch.countDown();
    }
  }
}
