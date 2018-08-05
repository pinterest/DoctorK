package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.servlet.DoctorKafkaWebServer;
import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.net.URL;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 *  DoctorKafka is the central service for managing kafka operation.
 *
 */
public class DoctorKafkaMain {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaMain.class);
  private static final String CONFIG_PATH = "config";
  private static final String METRICS_TOPIC = "topic";
  private static final String OSTRICH_PORT = "ostrichport";
  private static final String TSD_HOSTPORT = "tsdhostport";
  private static final String UPTIME_IN_SECONDS = "uptimeinseconds";
  private static final String ZOOKEEPER = "zookeeper";
  private static final Options options = new Options();

  public static  DoctorKafka doctorKafka = null;
  private static DoctorKafkaWatcher operatorWatcher = null;

  /**
   *  Usage:  DoctorKafkaMain  --config config_file_path
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option configPath = new Option(CONFIG_PATH, true, "config file path");
    Option zookeeper = new Option(ZOOKEEPER, true, "zk url for metrics topic");
    zookeeper.setRequired(false);
    Option topic = new Option(METRICS_TOPIC, true, "kafka topic for metric messages");
    topic.setRequired(false);
    Option tsdHostPort = new Option(TSD_HOSTPORT, true, "tsd host and port, e.g. localhost:18621");
    tsdHostPort.setRequired(false);
    Option ostrichPort = new Option(OSTRICH_PORT, true, "ostrich port");
    ostrichPort.setRequired(false);
    Option uptimeInSeconds = new Option(UPTIME_IN_SECONDS, true, "uptime in seconds");
    uptimeInSeconds.setRequired(false);
    options.addOption(configPath).addOption(zookeeper).addOption(topic)
        .addOption(tsdHostPort).addOption(ostrichPort).addOption(uptimeInSeconds);

    if (args.length < 1) {
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
    formatter.printHelp("DoctorKafka", options);
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    Runtime.getRuntime().addShutdownHook(new DoctorKafkaMain.OperatorCleanupThread());
    CommandLine commandLine = parseCommandLine(args);

    String configPath = commandLine.getOptionValue(CONFIG_PATH);
    LOG.info("configuration path : {}", configPath);

    ReplicaStatsManager.config =  new DoctorKafkaConfig(configPath);
    doctorKafka = new DoctorKafka(ReplicaStatsManager.config);
    doctorKafka.start();

    // start the web UI
    int webPort = ReplicaStatsManager.config.getWebserverPort();
    DoctorKafkaWebServer webServer = new DoctorKafkaWebServer(webPort);
    webServer.start();

    int ostrichPort = ReplicaStatsManager.config.getOstrichPort();
    String tsdHostPort = ReplicaStatsManager.config.getTsdHostPort();
    if (tsdHostPort == null && ostrichPort == 0) {
      LOG.warn("OpenTSDB and Ostrich options missing, not starting Ostrich service");
    } else if (ostrichPort == 0) {
      throw new NoSuchElementException(String.format("Key '%s' does not map to an existing object!", OSTRICH_PORT));
    } else {
      OperatorUtil.startOstrichService(tsdHostPort, ostrichPort);
    }
    LOG.info("DoctorKafka started.");
  }

  static class OperatorCleanupThread extends Thread {

    @Override
    public void run() {
      try {
        if (doctorKafka != null) {
          doctorKafka.stop();
        }
      } catch (Throwable t) {
        LOG.error("Failure in stopping operator", t);
      }

      try {
        if (operatorWatcher != null) {
          operatorWatcher.stop();
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure in collectorMonitor : ", t);
      }
    }
  }
}