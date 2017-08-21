package com.pinterest.doctorkafka.config;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DoctorKafkaConfig {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaConfig.class);
  private static final String DOCTORKAFKA_PREFIX = "doctorkafka.";
  private static final String CLUSTER_PREFIX = "kafkacluster.";

  private static final String BROKERSTATS_TOPIC = "brokerstats.topic";
  private static final String BROKERSTATS_VERSION = "brokerstats.version";
  private static final String BROKERSTATS_BACKTRACK_SECONDS = "brokerstats.backtrack.seconds";
  private static final String ACTION_REPORT_TOPIC = "action_report.topic";
  private static final String OSTRICH_PORT = "ostrich.port";
  private static final String RESTART_INTERVAL_SECONDS = "restart.interval.seconds";
  private static final String BROKERSTATS_ZOOKEEPER = "zookeeper";
  private static final String TSD_HOSTPORT = "tsd.hostport";
  private static final String WEB_PORT = "web.port";
  private static final String NOTIFICATION_EMAILS = "emails.notification";
  private static final String ALERT_EMAILS = "emails.alert";


  private PropertiesConfiguration configuration = null;
  private AbstractConfiguration operatorConfiguration = null;
  private Map<String, DoctorKafkaClusterConfig> clusterConfigurations = null;

  public DoctorKafkaConfig(String configPath) throws Exception {
    try {
      Configurations configurations = new Configurations();
      configuration = configurations.properties(new File(configPath));
      operatorConfiguration = new SubsetConfiguration(configuration, DOCTORKAFKA_PREFIX);
      this.initialize();
    } catch (Exception e) {
      LOG.error("Failed to initialize configuration file {}", configPath, e);
    }
  }

  private void initialize() {
    Set<String> clusters = new HashSet();
    Iterator<String> keysIterator = configuration.getKeys();

    while (keysIterator.hasNext()) {
      String propertyName = keysIterator.next();
      if (propertyName.startsWith(CLUSTER_PREFIX)) {
        String clusterName = propertyName.split("\\.")[1];
        clusters.add(clusterName);
      }
    }

    clusterConfigurations = new HashMap<>();
    for (String cluster : clusters) {
      SubsetConfiguration subsetConfiguration =
          new SubsetConfiguration(configuration, CLUSTER_PREFIX + cluster + ".");
      clusterConfigurations.put(
          cluster, new DoctorKafkaClusterConfig(cluster, subsetConfiguration));
    }
  }

  public Set<String> getClusters() {
    return clusterConfigurations.keySet();
  }

  public Set<String> getClusterZkUrls() {
    return clusterConfigurations.values().stream().map(clusterConfig -> clusterConfig.getZkUrl())
        .collect(Collectors.toSet());
  }

  public String getBrokerStatsTopic() {
    return operatorConfiguration.getString(BROKERSTATS_TOPIC);
  }

  public String getBrokerStatsVersion() {
    return operatorConfiguration.getString(BROKERSTATS_VERSION);
  }

  public long getBrokerStatsBacktrackWindowsInSeconds() {
    String backtrackWindow = operatorConfiguration.getString(BROKERSTATS_BACKTRACK_SECONDS);
    return Long.parseLong(backtrackWindow);
  }

  public String getActionReportTopic() {
    return operatorConfiguration.getString(ACTION_REPORT_TOPIC);
  }

  public String getTsdHostPort() {
    return operatorConfiguration.getString(TSD_HOSTPORT);
  }

  public int getOstrichPort() {
    return operatorConfiguration.getInt(OSTRICH_PORT);
  }

  public String getBrokerStatsZookeeper() {
    return operatorConfiguration.getString(BROKERSTATS_ZOOKEEPER);
  }

  public long getRestartIntervalInSeconds() {
    return operatorConfiguration.getLong(RESTART_INTERVAL_SECONDS);
  }

  public int getWebserverPort() {
    return operatorConfiguration.getInteger(WEB_PORT, 8080);
  }

  public DoctorKafkaClusterConfig getClusterConfigByZkUrl(String clusterZkUrl) {
    for (DoctorKafkaClusterConfig clusterConfig : clusterConfigurations.values()) {
      if (clusterConfig.getZkUrl().equals(clusterZkUrl)) {
        return clusterConfig;
      }
    }
    return null;
  }

  public DoctorKafkaClusterConfig getClusterConfigByName(String clusterName) {
    return clusterConfigurations.get(clusterName);
  }

  /**
   * The emails for sending notification. The message can be for informational purpose.
   */
  public String[] getNotificationEmails() {
    String emailsStr = operatorConfiguration.getString(NOTIFICATION_EMAILS);
    return emailsStr.split(",");
  }

  /**
   * The email addresses for sending alerts that the team needs to take actions.
   */
  public String[] getAlertEmails() {
    String emailsStr = operatorConfiguration.getString(ALERT_EMAILS);
    return emailsStr.split(",");
  }
}

