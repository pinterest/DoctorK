package com.pinterest.doctorkafka.config;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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
  private static final String BROKERSTATS_CONSUMER_PREFIX = "brokerstats.consumer.";
  private static final String ACTION_REPORT_PRODUCER_PREFIX = "action.report.producer.";
  private static final String SECURITY_PROTOCOL = "security.protocol";

  private static final String BROKERSTATS_ZKURL = "brokerstats.zkurl";
  private static final String BROKERSTATS_TOPIC = "brokerstats.topic";
  private static final String BROKERSTATS_VERSION = "brokerstats.version";
  private static final String BROKERSTATS_BACKTRACK_SECONDS = "brokerstats.backtrack.seconds";
  private static final String ACTION_REPORT_ZKURL= "action.report.zkurl";
  private static final String ACTION_REPORT_TOPIC = "action.report.topic";
  private static final String BROKER_REPLACEMENT_INTERVAL_SECONDS =
      "action.broker_replacement.interval.seconds";
  private static final String BROKER_REPLACEMENT_COMMAND = "action.broker_replacement.command";
  private static final String OSTRICH_PORT = "ostrich.port";
  private static final String RESTART_DISABLE = "restart.disabled";
  private static final String RESTART_INTERVAL_SECONDS = "restart.interval.seconds";
  private static final String DOCTORKAFKA_ZKURL = "zkurl";
  private static final String TSD_HOSTPORT = "tsd.hostport";
  private static final String WEB_PORT = "web.port";
  private static final String NOTIFICATION_EMAILS = "emails.notification";
  private static final String ALERT_EMAILS = "emails.alert";

  private PropertiesConfiguration configuration = null;
  private AbstractConfiguration drkafkaConfiguration = null;
  private Map<String, DoctorKafkaClusterConfig> clusterConfigurations = null;

  public DoctorKafkaConfig(String configPath) throws Exception {
    try {
      Configurations configurations = new Configurations();
      configuration = configurations.properties(new File(configPath));
      drkafkaConfiguration = new SubsetConfiguration(configuration, DOCTORKAFKA_PREFIX);
      this.initialize();
    } catch (Exception e) {
      LOG.error("Failed to initialize configuration file {}", configPath, e);
    }
  }

  private void initialize() {
    Set<String> clusters = new HashSet<>();
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

  public String getDoctorKafkaZkurl() {
    return drkafkaConfiguration.getString(DOCTORKAFKA_ZKURL);
  }

  public String getBrokerstatsZkurl() {
    return drkafkaConfiguration.getString(BROKERSTATS_ZKURL);
  }

  public String getBrokerStatsTopic() {
    return drkafkaConfiguration.getString(BROKERSTATS_TOPIC);
  }

  public String getBrokerStatsVersion() {
    return drkafkaConfiguration.getString(BROKERSTATS_VERSION);
  }

  public long getBrokerStatsBacktrackWindowsInSeconds() {
    String backtrackWindow = drkafkaConfiguration.getString(BROKERSTATS_BACKTRACK_SECONDS);
    return Long.parseLong(backtrackWindow);
  }

  /**
   * This method parses the configuration file and returns the kafka producer ssl setting
   * for writing to brokerstats kafka topic
   */
  public Map<String, String> getBrokerStatsConsumerSslConfigs() {
    AbstractConfiguration sslConfiguration = new SubsetConfiguration(drkafkaConfiguration, BROKERSTATS_CONSUMER_PREFIX);
    return configurationToMap(sslConfiguration);
  }

  public SecurityProtocol getBrokerStatsConsumerSecurityProtocol() {
    Map<String, String> sslConfigMap = getBrokerStatsConsumerSslConfigs();
    return sslConfigMap.containsKey(SECURITY_PROTOCOL)
        ?  Enum.valueOf(SecurityProtocol.class, sslConfigMap.get(SECURITY_PROTOCOL)) : SecurityProtocol.PLAINTEXT;
  }

  public String getActionReportZkurl() {
    return drkafkaConfiguration.getString(ACTION_REPORT_ZKURL);
  }

  public String getActionReportTopic() {
    return drkafkaConfiguration.getString(ACTION_REPORT_TOPIC);
  }

  public Map<String, String> getActionReportProducerSslConfigs() {
    AbstractConfiguration sslConfiguration =
        new SubsetConfiguration(drkafkaConfiguration, ACTION_REPORT_PRODUCER_PREFIX);
    return configurationToMap(sslConfiguration);
  }

  public SecurityProtocol getActionReportProducerSecurityProtocol() {
    Map<String, String> sslConfigMap = getActionReportProducerSslConfigs();
    return sslConfigMap.containsKey(SECURITY_PROTOCOL)
        ?  Enum.valueOf(SecurityProtocol.class, sslConfigMap.get(SECURITY_PROTOCOL)) : SecurityProtocol.PLAINTEXT;
  }


  protected static Map<String, String> configurationToMap(AbstractConfiguration  configuration) {
    Iterator<String> keysIterator = configuration.getKeys();
    Map<String, String> result = new HashMap<>();
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();
      result.put(key, configuration.getString(key));
    }
    return result;
  }

  public int getBrokerReplacementIntervalInSeconds() {
    return drkafkaConfiguration.getInt(BROKER_REPLACEMENT_INTERVAL_SECONDS, 43200);
  }

  public String getBrokerReplacementCommand() {
    String command = drkafkaConfiguration.getString(BROKER_REPLACEMENT_COMMAND);
    command = command.replaceAll("^\"|\"$", "");
    return command;
  }

  public String getTsdHostPort() {
    return drkafkaConfiguration.getString(TSD_HOSTPORT);
  }

  public int getOstrichPort() {
    return drkafkaConfiguration.getInt(OSTRICH_PORT, 0);
  }

  public long getRestartIntervalInSeconds() {
    return drkafkaConfiguration.getLong(RESTART_INTERVAL_SECONDS);
  }

  public int getWebserverPort() {
    return drkafkaConfiguration.getInteger(WEB_PORT, 8080);
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
   * @return an array of email addresses for sending notification to.
   */
  public String[] getNotificationEmails() {
    String emailsStr = drkafkaConfiguration.getString(NOTIFICATION_EMAILS);
    return emailsStr.split(",");
  }

  /**
   * The email addresses for sending alerts that the team needs to take actions.
   * @return an array of email addresses for sending alerts to.
   */
  public String[] getAlertEmails() {
    String emailsStr = drkafkaConfiguration.getString(ALERT_EMAILS);
    return emailsStr.split(",");
  }

  public boolean getRestartDisabled(){
    return drkafkaConfiguration.getBoolean(RESTART_DISABLE, false);
  }
}