package com.pinterest.doctorkafka.stats;


import com.pinterest.doctorkafka.util.MetricsPusher;
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

/**
 * StatsCollectorMain collects all kafka operation related metrics from a kafka broker, and send
 * the metrics to a kafka topic. The following shows some sample metrics that we need to collect:
 *
 *  kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=ReplicaFetcherThread-0-6033, \
 *     topic=ems_processing_log,partition=4
 *
 */
public class KafkaStatsMain {

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

  protected static final String hostName = OperatorUtil.getHostname();
  private static final Options options = new Options();
  private static MetricsPusher metricsPusher = null;
  private static final int TSDB_METRICS_PUSH_INTERVAL_IN_MILLISECONDS = 10 * 1000;

  /**
   *  Usage:  com.pinterest.kafka.KafkaStatsMain  --host kafkahost --port 9999
   *             --zookeeper datazk001:2181/data05  --topic  kafka_metrics
   *             --tsdhost  localhost:18321  --ostrichport 2051
   *             --uptimeinseconds 43200 --pollinginterval 15
   *             --kafka_config  /etc/kafka/server.properties
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

    options.addOption(jmxPort).addOption(host).addOption(zookeeper).addOption(topic)
        .addOption(tsdHostPort).addOption(ostrichPort).addOption(uptimeInSeconds)
        .addOption(pollingInterval).addOption(kafkaConfig);

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

    brokerStatsReporter = new BrokerStatsReporter(
        kafkaConfigPath, host, jmxPort, destTopic, zkUrl, pollingInterval);
    brokerStatsReporter.start();

    collectorMonitor = new CollectorMonitor(uptimeInSeconds);
    collectorMonitor.start();
    if (tsdHostPort == null && ostrichPort == null) {
      LOG.info("OpenTSDB and Ostrich options missing, not starting Ostrich service");
    } else {
      OperatorUtil.startOstrichService(tsdHostPort, Integer.parseInt(ostrichPort));
    }
  }


  public static class CollectorMonitor implements Runnable  {

    private static final Logger LOG = LogManager.getLogger(CollectorMonitor.class);
    private static final int INITIAL_DELAY = 0;
    /**
     * The executor service for executing MercedWorkerMonitor thread
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
    }
  }
}
