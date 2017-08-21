package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.replicastats.BrokerStatsProcessor;
import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DoctorKafka {

  private static final Logger LOG = LogManager.getLogger(DoctorKafka.class);

  private DoctorKafkaConfig operatorConf;

  public BrokerStatsProcessor brokerStatsProcessor = null;

  private DoctorKafkaActionReporter actionReporter = null;

  private List<KafkaClusterManager> clusterManagers = new ArrayList();

  private Set<String> clusterZkUrls = null;

  public DoctorKafka(DoctorKafkaConfig operatorConf) {
    this.operatorConf = operatorConf;
    this.clusterZkUrls = operatorConf.getClusterZkUrls();
  }


  public void start() {
    String zkUrl = operatorConf.getBrokerStatsZookeeper();
    String statsTopic = operatorConf.getBrokerStatsTopic();
    String actionReportTopic = operatorConf.getActionReportTopic();

    LOG.info("Start rebuilding the replica stats by reading the past 24 hours brokerstats");
    ReplicaStatsManager.readPastReplicaStats(zkUrl,
        operatorConf.getBrokerStatsTopic(), operatorConf.getBrokerStatsBacktrackWindowsInSeconds());
    LOG.info("Finish rebuilding the replica stats");

    brokerStatsProcessor = new BrokerStatsProcessor(zkUrl, statsTopic);
    brokerStatsProcessor.start();

    actionReporter = new DoctorKafkaActionReporter(actionReportTopic, zkUrl);
    for (String clusterZkUrl : clusterZkUrls) {
      DoctorKafkaClusterConfig clusterConf = operatorConf.getClusterConfigByZkUrl(clusterZkUrl);
      KafkaCluster kafkaCluster = ReplicaStatsManager.clusters.get(clusterZkUrl);

      if (kafkaCluster == null) {
        LOG.error("No brokerstats info for cluster {}", clusterZkUrl);
        continue;
      }
      KafkaClusterManager clusterManager = new KafkaClusterManager(
          clusterZkUrl, kafkaCluster, clusterConf, operatorConf, actionReporter);
      clusterManagers.add(clusterManager);
      clusterManager.start();
      LOG.info("Starting cluster manager for " + clusterZkUrl);
    }
  }

  public void stop() {
    brokerStatsProcessor.stop();
    for (KafkaClusterManager clusterManager : clusterManagers) {
      clusterManager.stop();
    }
  }

  public DoctorKafkaConfig getOperatorConfig() {
    return operatorConf;
  }

  public List<KafkaClusterManager> getClusterManagers() {
    return clusterManagers;
  }

  public KafkaClusterManager getClusterManager(String clusterName) {
    for (KafkaClusterManager clusterManager : clusterManagers) {
      if (clusterManager.getClusterName().equals(clusterName))
        return clusterManager;
    }
    return null;
  }
}
