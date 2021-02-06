package com.pinterest.doctork.config;

import com.pinterest.doctork.security.DoctorKAuthorizationFilter;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DoctorKConfig {

  private static final Logger LOG = LogManager.getLogger(DoctorKConfig.class);
  private static final String DOCTORK_PREFIX = "doctork.";
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
  private static final String DOCTORK_ZKURL = "zkurl";
  private static final String TSD_HOSTPORT = "tsd.hostport";
  private static final String WEB_PORT = "web.port";
  private static final String NOTIFICATION_EMAILS = "emails.notification";
  private static final String ALERT_EMAILS = "emails.alert";
  private static final String WEB_BIND_HOST = "web.bindhost";
  public static final String DOCTORK_ADMIN_ROLE = "doctork_admin";
  private static final String DOCTORK_ADMIN_GROUPS = "admin.groups";
  private static final String AUTHORIZATION_FILTER_CLASS = "authorization.filter.class";

  private PropertiesConfiguration configuration = null;
  private AbstractConfiguration doctorKConfiguration = null;
  private Map<String, DoctorKClusterConfig> clusterConfigurations = null;

  public DoctorKConfig(String configPath) throws Exception {
    try {
      Configurations configurations = new Configurations();
      configuration = configurations.properties(new File(configPath));
      doctorKConfiguration = new SubsetConfiguration(configuration, DOCTORK_PREFIX);
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
          cluster, new DoctorKClusterConfig(cluster, subsetConfiguration));
    }
  }

  public Set<String> getClusters() {
    return clusterConfigurations.keySet();
  }

  public Set<String> getClusterZkUrls() {
    return clusterConfigurations.values().stream().map(clusterConfig -> clusterConfig.getZkUrl())
        .collect(Collectors.toSet());
  }

  public String getDoctorKZkurl() {
    return doctorKConfiguration.getString(DOCTORK_ZKURL);
  }

  public String getBrokerstatsZkurl() {
    return doctorKConfiguration.getString(BROKERSTATS_ZKURL);
  }

  public String getBrokerStatsTopic() {
    return doctorKConfiguration.getString(BROKERSTATS_TOPIC);
  }

  public String getBrokerStatsVersion() {
    return doctorKConfiguration.getString(BROKERSTATS_VERSION);
  }

  public long getBrokerStatsBacktrackWindowsInSeconds() {
    String backtrackWindow = doctorKConfiguration.getString(BROKERSTATS_BACKTRACK_SECONDS);
    return Long.parseLong(backtrackWindow);
  }

  /**
   * This method parses the configuration file and returns the kafka producer ssl setting
   * for writing to brokerstats kafka topic
   */
  public Map<String, String> getBrokerStatsConsumerSslConfigs() {
    AbstractConfiguration sslConfiguration = new SubsetConfiguration(doctorKConfiguration, BROKERSTATS_CONSUMER_PREFIX);
    return configurationToMap(sslConfiguration);
  }

  public SecurityProtocol getBrokerStatsConsumerSecurityProtocol() {
    Map<String, String> sslConfigMap = getBrokerStatsConsumerSslConfigs();
    return sslConfigMap.containsKey(SECURITY_PROTOCOL)
        ?  Enum.valueOf(SecurityProtocol.class, sslConfigMap.get(SECURITY_PROTOCOL)) : SecurityProtocol.PLAINTEXT;
  }

  public String getActionReportZkurl() {
    return doctorKConfiguration.getString(ACTION_REPORT_ZKURL);
  }

  public String getActionReportTopic() {
    return doctorKConfiguration.getString(ACTION_REPORT_TOPIC);
  }

  public Map<String, String> getActionReportProducerSslConfigs() {
    AbstractConfiguration sslConfiguration =
        new SubsetConfiguration(doctorKConfiguration, ACTION_REPORT_PRODUCER_PREFIX);
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
    return doctorKConfiguration.getInt(BROKER_REPLACEMENT_INTERVAL_SECONDS, 43200);
  }

  public String getBrokerReplacementCommand() {
    String command = doctorKConfiguration.getString(BROKER_REPLACEMENT_COMMAND);
    command = command.replaceAll("^\"|\"$", "");
    return command;
  }

  public String getTsdHostPort() {
    return doctorKConfiguration.getString(TSD_HOSTPORT);
  }

  public int getOstrichPort() {
    return doctorKConfiguration.getInt(OSTRICH_PORT, 0);
  }

  public long getRestartIntervalInSeconds() {
    return doctorKConfiguration.getLong(RESTART_INTERVAL_SECONDS);
  }

  public int getWebserverPort() {
    return doctorKConfiguration.getInteger(WEB_PORT, 8080);
  }
  
  public String getWebserverBindHost() {
    return doctorKConfiguration.getString(WEB_BIND_HOST, "0.0.0.0");
  }

  public DoctorKClusterConfig getClusterConfigByZkUrl(String clusterZkUrl) {
    for (DoctorKClusterConfig clusterConfig : clusterConfigurations.values()) {
      if (clusterConfig.getZkUrl().equals(clusterZkUrl)) {
        return clusterConfig;
      }
    }
    return null;
  }

  public DoctorKClusterConfig getClusterConfigByName(String clusterName) {
    return clusterConfigurations.get(clusterName);
  }

  /**
   * The emails for sending notification. The message can be for informational purpose.
   * @return an array of email addresses for sending notification to.
   */
  public String[] getNotificationEmails() {
    String emailsStr = doctorKConfiguration.getString(NOTIFICATION_EMAILS);
    return emailsStr.split(",");
  }

  /**
   * The email addresses for sending alerts that the team needs to take actions.
   * @return an array of email addresses for sending alerts to.
   */
  public String[] getAlertEmails() {
    String emailsStr = doctorKConfiguration.getString(ALERT_EMAILS);
    return emailsStr.split(",");
  }

  public boolean getRestartDisabled(){
    return doctorKConfiguration.getBoolean(RESTART_DISABLE, false);
  }
  
  /**
   * Return authorization filter class (if any)
   * @return authorization filter class
   * @throws ClassNotFoundException 
   */
  @SuppressWarnings("unchecked")
  public Class<? extends DoctorKAuthorizationFilter> getAuthorizationFilterClass() throws ClassNotFoundException {
    if (doctorKConfiguration.containsKey(AUTHORIZATION_FILTER_CLASS)) {
      String classFqcn = doctorKConfiguration.getString(AUTHORIZATION_FILTER_CLASS);
      return (Class<? extends DoctorKAuthorizationFilter>) Class.forName(classFqcn);
    } else {
      return null;
    }
  }
  
  /**
   * Groups from directory service (like LDAP) that are granted Dr.Kafka Admin 
   * permissions to run privileged commands.
   * @return list of groups
   */
  public List<String> getDoctorKAdminGroups() {
    if (doctorKConfiguration.containsKey(DOCTORK_ADMIN_GROUPS)) {
      return Arrays.asList(doctorKConfiguration.getStringArray(DOCTORK_ADMIN_GROUPS));
    } else {
      return null; 
    }
  }
}