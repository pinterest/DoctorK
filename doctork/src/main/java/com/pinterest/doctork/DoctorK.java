package com.pinterest.doctork;

import com.pinterest.doctork.config.DoctorKClusterConfig;
import com.pinterest.doctork.config.DoctorKConfig;
import com.pinterest.doctork.replicastats.BrokerStatsProcessor;
import com.pinterest.doctork.replicastats.ReplicaStatsManager;
import com.pinterest.doctork.util.ZookeeperClient;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DoctorK {

  private static final Logger LOG = LogManager.getLogger(DoctorK.class);

  private DoctorKConfig doctorKConf;

  private BrokerStatsProcessor brokerStatsProcessor = null;

  private DoctorKActionReporter actionReporter = null;

  private Map<String, KafkaClusterManager> clusterManagers = new HashMap<>();

  private Set<String> clusterZkUrls = null;

  private ZookeeperClient zookeeperClient = null;

  private DoctorKHeartbeat heartbeat = null;

  private ReplicaStatsManager replicaStatsManager = null;

  public DoctorK(ReplicaStatsManager replicaStatsManager) {
    this.replicaStatsManager = replicaStatsManager;
    this.doctorKConf = replicaStatsManager.getConfig();
    this.clusterZkUrls = doctorKConf.getClusterZkUrls();
    this.zookeeperClient = new ZookeeperClient(doctorKConf.getDoctorKZkurl());
  }

  public void start() {
    String brokerstatsZkurl = doctorKConf.getBrokerstatsZkurl();
    String actionReportZkurl = doctorKConf.getActionReportZkurl();
    String statsTopic = doctorKConf.getBrokerStatsTopic();
    SecurityProtocol statsSecurityProtocol = doctorKConf.getBrokerStatsConsumerSecurityProtocol();
    String actionReportTopic = doctorKConf.getActionReportTopic();
    SecurityProtocol actionReportSecurityProtocol = doctorKConf.getActionReportProducerSecurityProtocol();

    LOG.info("Start rebuilding the replica stats by reading the past 24 hours brokerstats");
    replicaStatsManager.readPastReplicaStats(brokerstatsZkurl, statsSecurityProtocol,
        doctorKConf.getBrokerStatsTopic(), doctorKConf.getBrokerStatsBacktrackWindowsInSeconds());
    LOG.info("Finish rebuilding the replica stats");

    brokerStatsProcessor = new BrokerStatsProcessor(brokerstatsZkurl, statsSecurityProtocol, statsTopic,
        doctorKConf.getBrokerStatsConsumerSslConfigs(), replicaStatsManager);
    brokerStatsProcessor.start();

    actionReporter = new DoctorKActionReporter(actionReportZkurl, actionReportSecurityProtocol, actionReportTopic,
        doctorKConf.getActionReportProducerSslConfigs());
    for (String clusterZkUrl : clusterZkUrls) {
      DoctorKClusterConfig clusterConf = doctorKConf.getClusterConfigByZkUrl(clusterZkUrl);
      KafkaCluster kafkaCluster = replicaStatsManager.getClusters().get(clusterZkUrl);

      if (kafkaCluster == null) {
        LOG.error("No brokerstats info for cluster {}", clusterZkUrl);
        continue;
      }
      KafkaClusterManager clusterManager = new KafkaClusterManager(
          clusterZkUrl, kafkaCluster, clusterConf, doctorKConf, actionReporter, zookeeperClient, replicaStatsManager);
      clusterManagers.put(clusterConf.getClusterName(), clusterManager);
      clusterManager.start();
      LOG.info("Starting cluster manager for " + clusterZkUrl);
    }

    heartbeat = new DoctorKHeartbeat();
    heartbeat.start();
  }

  public void stop() {
    brokerStatsProcessor.stop();
    zookeeperClient.close();
    heartbeat.stop();
    for (KafkaClusterManager clusterManager : clusterManagers.values()) {
      clusterManager.stop();
    }
  }

  public DoctorKConfig getDoctorKConfig() {
    return doctorKConf;
  }

  public Collection<KafkaClusterManager> getClusterManagers() {
    return clusterManagers.values();
  }

  public KafkaClusterManager getClusterManager(String clusterName) {
    return clusterManagers.get(clusterName);
  }

  public List<String> getClusterNames() {
    return new ArrayList<>(clusterManagers.keySet());
  }

}
