package com.pinterest.doctorkafka.config;

import com.pinterest.doctorkafka.security.DrKafkaAuthorizationFilter;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class DoctorKafkaConfig {
  public static final String MONITORS_PREFIX = "monitors";
  public static final String ACTIONS_PREFIX = "actions";
  public static final String OPERATORS_PREFIX = "operators";
  public static final String NAME_KEY = "name";

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaConfig.class);
  private static final String DOCTORKAFKA_PREFIX = "doctorkafka";
  private static final String CLUSTER_PREFIX = "kafkaclusters";
  private static final String ACTION_REPORT_TOPIC = "action_report.topic";
  private static final String ACTION_REPORT_ZKURL = "action_report.zkurl";
  private static final String ACTION_REPORT_CONSUMER_CONFIG = "action_report.consumer_config";
  private static final String SECURITY_PROTOCOL = "security.protocol";
  private static final String RESTART_DISABLE = "restart.disabled";
  private static final String RESTART_INTERVAL_SECONDS = "restart.interval_seconds";
  private static final String DOCTORKAFKA_ZKURL = "zkurl";
  private static final String TSD_HOST = "tsd.host";
  private static final String TSD_PORT = "tsd.port";
  private static final String OSTRICH_PORT = "ostrich.port";
  private static final String UI_HOST = "ui.host";
  private static final String UI_PORT = "ui.port";
  public static final String DRKAFKA_ADMIN_ROLE = "drkafka_admin";
  private static final String DRKAFKA_ADMIN_GROUPS = "admin.groups";
  private static final String AUTHORIZATION_FILTER_CLASS = "authorization.filter.class";
  private static final String EVALUATION_FREQUENCY_SECONDS = "evaluation_interval_seconds";

  private static final int DEFAULT_EVALUATION_FREQUENCY_SECONDS = 5;

  private AbstractConfiguration drkafkaConfiguration;
  private ConcurrentMap<String, Configuration> monitorConfigs = new ConcurrentHashMap<>();
  private ConcurrentMap<String, Configuration> operatorConfigs = new ConcurrentHashMap<>();
  private ConcurrentMap<String, Configuration> actionConfigs = new ConcurrentHashMap<>();
  private ConcurrentMap<String, DoctorKafkaClusterConfig> clusterConfigs = new ConcurrentHashMap<>();

  public DoctorKafkaConfig(String configPath) throws Exception {
    try {
      Parameters params = new Parameters();
      FileBasedConfigurationBuilder<YAMLConfiguration> builder =
          new FileBasedConfigurationBuilder<>(YAMLConfiguration.class)
              .configure(params.hierarchical().setFileName(configPath));
      YAMLConfiguration configuration = builder.getConfiguration();

      drkafkaConfiguration = (AbstractConfiguration) configuration.configurationAt(DOCTORKAFKA_PREFIX);

      String name;
      for(HierarchicalConfiguration monitorConfig : configuration.configurationsAt(MONITORS_PREFIX)){
        name = monitorConfig.getString(NAME_KEY);
        monitorConfigs.put(name, monitorConfig);
      }

      for(HierarchicalConfiguration operatorConfig : configuration.configurationsAt(OPERATORS_PREFIX)){
        name = operatorConfig.getString(NAME_KEY);
        operatorConfigs.put(name, operatorConfig);
      }

      for(HierarchicalConfiguration actionConfig : configuration.configurationsAt(ACTIONS_PREFIX)){
        name = actionConfig.getString(NAME_KEY);
        actionConfigs.put(name, actionConfig);
      }

      for(HierarchicalConfiguration clusterConfig : configuration.configurationsAt(CLUSTER_PREFIX)){
        name = clusterConfig.getString(NAME_KEY);
        clusterConfigs.put(name, new DoctorKafkaClusterConfig(name, clusterConfig));

      }
    } catch (Exception e) {
      LOG.error("Failed to initialize configuration file {}", configPath, e);
    }
  }

  public Map<String, Configuration> getMonitorsConfigs() {
    return monitorConfigs;
  }

  public Map<String, Configuration> getOperatorsConfigs() {
    return operatorConfigs;
  }

  public Map<String, Configuration> getActionsConfigs() {
    return actionConfigs;
  }

  public Set<String> getClusterZkUrls() {
    return clusterConfigs.values().stream().map(DoctorKafkaClusterConfig::getZkUrl)
        .collect(Collectors.toSet());
  }

  public String getDoctorKafkaZkurl() {
    return drkafkaConfiguration.getString(DOCTORKAFKA_ZKURL);
  }

  public String getActionReportZkurl() {
    return drkafkaConfiguration.getString(ACTION_REPORT_ZKURL);
  }

  public String getActionReportTopic() {
    return drkafkaConfiguration.getString(ACTION_REPORT_TOPIC);
  }

  public SecurityProtocol getActionReportSecurityProtocol() throws IOException {
    Properties consumerConfig = getActionReportConsumerConfig();

    SecurityProtocol securityProtocol = consumerConfig.containsKey(SECURITY_PROTOCOL) ?
                       Enum.valueOf(SecurityProtocol.class, consumerConfig.getProperty(SECURITY_PROTOCOL)) :
                       SecurityProtocol.PLAINTEXT;

    return securityProtocol;
  }

  public Properties getActionReportConsumerConfig() throws IOException {
    String configStr = drkafkaConfiguration.getString(ACTION_REPORT_CONSUMER_CONFIG);
    Properties properties = new Properties();
    properties.load(new StringReader(configStr));
    return properties;
  }

  public String getTsdHost() {
    return drkafkaConfiguration.getString(TSD_HOST);
  }

  public int getTsdPort(){
    return drkafkaConfiguration.getInt(TSD_PORT, 0);
  }

  public int getOstrichPort() {
    return drkafkaConfiguration.getInt(OSTRICH_PORT, 0);
  }

  public long getRestartIntervalInSeconds() {
    return drkafkaConfiguration.getLong(RESTART_INTERVAL_SECONDS);
  }

  public int getWebserverPort() {
    return drkafkaConfiguration.getInteger(UI_PORT, 8080);
  }
  
  public String getWebserverBindHost() {
    return drkafkaConfiguration.getString(UI_HOST, "0.0.0.0");
  }

  public DoctorKafkaClusterConfig getClusterConfigByZkUrl(String clusterZkUrl) {
    for (DoctorKafkaClusterConfig clusterConfig : clusterConfigs.values()) {
      if (clusterConfig.getZkUrl().equals(clusterZkUrl)) {
        return clusterConfig;
      }
    }
    return null;
  }

  public DoctorKafkaClusterConfig getClusterConfigByName(String clusterName) {
    return clusterConfigs.get(clusterName);
  }

  public boolean getRestartDisabled(){
    return drkafkaConfiguration.getBoolean(RESTART_DISABLE, false);
  }
  
  /**
   * Return authorization filter class (if any)
   * @return authorization filter class
   * @throws ClassNotFoundException 
   */
  @SuppressWarnings("unchecked")
  public Class<? extends DrKafkaAuthorizationFilter> getAuthorizationFilterClass() throws ClassNotFoundException {
    if (drkafkaConfiguration.containsKey(AUTHORIZATION_FILTER_CLASS)) {
      String classFqcn = drkafkaConfiguration.getString(AUTHORIZATION_FILTER_CLASS);
      return (Class<? extends DrKafkaAuthorizationFilter>) Class.forName(classFqcn);
    } else {
      return null;
    }
  }
  
  /**
   * Groups from directory service (like LDAP) that are granted Dr.Kafka Admin 
   * permissions to run privileged commands.
   * @return list of groups
   */
  public List<String> getDrKafkaAdminGroups() {
    if (drkafkaConfiguration.containsKey(DRKAFKA_ADMIN_GROUPS)) {
      return Arrays.asList(drkafkaConfiguration.getStringArray(DRKAFKA_ADMIN_GROUPS));
    } else {
      return null; 
    }
  }

  public Long getEvaluationFrequency(){
    return drkafkaConfiguration.getInt(EVALUATION_FREQUENCY_SECONDS, DEFAULT_EVALUATION_FREQUENCY_SECONDS) * 1000L;
  }
}