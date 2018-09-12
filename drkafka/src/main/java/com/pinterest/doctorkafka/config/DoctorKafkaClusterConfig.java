package com.pinterest.doctorkafka.config;


import org.apache.commons.configuration2.AbstractConfiguration;


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

  private static final String DRYRUN = "dryrun";
  private static final String ZKURL = "zkurl";
  private static final String ENABLE_WORLOAD_BALANCING = "balance_workload.enabled";
  private static final String NETWORK_IN_LIMIT_MB = "network.inbound.limit.mb";
  private static final String NETWORK_OUT_MB = "network.outbound.limit.mb";
  private static final String NETWORK_BANDWITH_MB = "network.bandwidth.max.mb";
  private static final String REPLICA_MIN_COUNT = "replica.min";
  private static final String REPLICA_MAX_COUNT = "replica.max";
  private static final String CHECK_INTERVAL_IN_SECS = "check_interval_in_seconds";
  private static final String UNDER_REPLICTED_ALERT_IN_SECS = "under_replicated.alert.seconds";
  private static final String BROKER_REPLACEMENT_ENABLE = "broker_replacement.enable";
  private static final String BROKER_REPLACEMENT_NO_STATS_SECONDS =
      "broker_replacement.no_stats.seconds";
  private static final String NOTIFICATION_EMAIL = "notification.email";
  private static final String NOTIFICATION_PAGER = "notificatino.pager";

  private static final int DEFAULT_DEADBROKER_REPLACEMENT_NO_STATS_SECONDS = 1200;
  private static final int DEFAULT_UNDER_REPLICTED_ALERT_IN_SECS = 7200;

  private String clusterName;
  private AbstractConfiguration clusterConfiguration;

  public DoctorKafkaClusterConfig(String clusterName, AbstractConfiguration configuration) {
    this.clusterName = clusterName;
    this.clusterConfiguration = configuration;
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public boolean dryRun() {
    return clusterConfiguration.getBoolean(DRYRUN);
  }

  public String getZkUrl() {
    return clusterConfiguration.getString(ZKURL);
  }

  public boolean enabledWorloadBalancing() {
    boolean result = false;
    if (clusterConfiguration.containsKey(ENABLE_WORLOAD_BALANCING)) {
      result = clusterConfiguration.getBoolean(ENABLE_WORLOAD_BALANCING);
    }
    return result;
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

  public double getNetworkBandwidthInMb() {
    return clusterConfiguration.getDouble(NETWORK_BANDWITH_MB);
  }

  public double getNetworkBandwidthInBytes() {
    return getNetworkBandwidthInMb() * 1024.0 * 1024.0;
  }

  public int getReplicaCountMin() {
    return clusterConfiguration.getInt(REPLICA_MIN_COUNT, 0);
  }

  public int getReplicaCountMax() {
    return clusterConfiguration.getInt(REPLICA_MAX_COUNT, 0);
  }

  public int getCheckIntervalInSeconds() {
    return clusterConfiguration.getInt(CHECK_INTERVAL_IN_SECS);
  }

  public int getUnderReplicatedAlertTimeInSeconds() {
    return clusterConfiguration.getInteger(UNDER_REPLICTED_ALERT_IN_SECS,
        DEFAULT_UNDER_REPLICTED_ALERT_IN_SECS);
  }

  public long getUnderReplicatedAlertTimeInMs() {
    return getUnderReplicatedAlertTimeInSeconds() * 1000L;
  }

  public boolean enabledDeadbrokerReplacement() {
    boolean result = false;
    if (clusterConfiguration.containsKey(BROKER_REPLACEMENT_ENABLE)) {
      result = clusterConfiguration.getBoolean(BROKER_REPLACEMENT_ENABLE);
    }
    return result;
  }

  public int getBrokerReplacementNoStatsSeconds() {
    int result = clusterConfiguration.getInt(BROKER_REPLACEMENT_NO_STATS_SECONDS,
        DEFAULT_DEADBROKER_REPLACEMENT_NO_STATS_SECONDS);
    return result;
  }

  public String getNotificationEmail() {
    return clusterConfiguration.getString(NOTIFICATION_EMAIL, "");
  }

  public String getNotificationPager() {
    return clusterConfiguration.getString(NOTIFICATION_PAGER, "");
  }
}
