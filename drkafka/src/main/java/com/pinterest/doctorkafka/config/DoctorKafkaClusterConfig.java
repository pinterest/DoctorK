package com.pinterest.doctorkafka.config;


import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;

/**
 * kafkacluster.data07.dryrun=true
 * kafkacluster.data07.zkurl=datazk001:2181,datazk002:2181,/data07
 * kafkacluster.data07.peak_to_mean_ratio=3.5
 * kafkacluster.data07.threshold.cpu=0.32
 * kafkacluster.data07.threshold.network.inbound.mb=50
 * kafkacluster.data07.threshold.network.outbound.mb=150
 * kafkacluster.data07.threshold.disk.usage=0.75
 *
 */
public class DoctorKafkaClusterConfig {

  private static final String ZKURL = "zkurl";
  private static final String NETWORK_IN_LIMIT_MB = "network.inbound.limit.mb";
  private static final String NETWORK_OUT_MB = "network.outbound.limit.mb";

  private String clusterName;
  private AbstractConfiguration clusterConfiguration;

  public DoctorKafkaClusterConfig(String clusterName, AbstractConfiguration configuration) {
    this.clusterName = clusterName;
    this.clusterConfiguration = configuration;
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getZkUrl() {
    return clusterConfiguration.getString(ZKURL);
  }

  public double getNetworkInLimitInMb() {
    return clusterConfiguration.getDouble(NETWORK_IN_LIMIT_MB);
  }

  public double getNetworkInLimitInBytes() {
    return getNetworkInLimitInMb() * 1024.0 * 1024.0;
  }

  public double getNetworkOutLimitInMb() {
    return clusterConfiguration.getDouble(NETWORK_OUT_MB);
  }

  public double getNetworkOutLimitInBytes() {
    return getNetworkOutLimitInMb() * 1024.0 * 1024.0;
  }

  public String[] getEnabledMonitors() {
    if (clusterConfiguration.containsKey(DoctorKafkaConfig.ENABLED_MONITORS)) {
      return clusterConfiguration.getStringArray((DoctorKafkaConfig.ENABLED_MONITORS));
    }
    return null;
  }

  public String[] getEnabledOperators() {
    if (clusterConfiguration.containsKey(DoctorKafkaConfig.ENABLED_OPERATORS)) {
      return clusterConfiguration.getStringArray((DoctorKafkaConfig.ENABLED_OPERATORS));
    }
    return null;
  }

  public String[] getEnabledActions() {
    if (clusterConfiguration.containsKey(DoctorKafkaConfig.ENABLED_ACTIONS)) {
      return clusterConfiguration.getStringArray((DoctorKafkaConfig.ENABLED_ACTIONS));
    }
    return null;
  }

  public AbstractConfiguration getMonitorConfiguration(String moduleName) {
    return new SubsetConfiguration(clusterConfiguration, DoctorKafkaConfig.MONITORS_PREFIX + moduleName);
  }

  public AbstractConfiguration getActionConfiguration(String moduleName) {
    return new SubsetConfiguration(clusterConfiguration, DoctorKafkaConfig.ACTIONS_PREFIX + moduleName);
  }

  public AbstractConfiguration getOperatorConfiguration(String moduleName) {
    return new SubsetConfiguration(clusterConfiguration, DoctorKafkaConfig.OPERATORS_PREFIX + moduleName);
  }
}
