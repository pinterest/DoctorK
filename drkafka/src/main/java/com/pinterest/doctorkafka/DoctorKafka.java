package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.replicastats.BrokerStatsProcessor;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DoctorKafka {

  private static final Logger LOG = LogManager.getLogger(DoctorKafka.class);

  private DoctorKafkaConfig drkafkaConf;

  public BrokerStatsProcessor brokerStatsProcessor = null;

  private DoctorKafkaActionReporter actionReporter = null;

  private List<KafkaClusterManager> clusterManagers = new ArrayList();

  private Set<String> clusterZkUrls = null;

  private ZookeeperClient zookeeperClient = null;

  public DoctorKafka(DoctorKafkaConfig drkafkaConf) {
    this.drkafkaConf = drkafkaConf;
    this.clusterZkUrls = drkafkaConf.getClusterZkUrls();
    this.zookeeperClient = new ZookeeperClient(drkafkaConf.getDoctorKafkaZkurl());
  }
  
  public void start() {
    String brokerstatsZkurl = drkafkaConf.getBrokerstatsZkurl();
    String actionReportZkurl = drkafkaConf.getActionReportZkurl();
    String statsTopic = drkafkaConf.getBrokerStatsTopic();
    SecurityProtocol statsSecurityProtocol = drkafkaConf.getBrokerStatsConsumerSecurityProtocol();
    String actionReportTopic = drkafkaConf.getActionReportTopic();
    SecurityProtocol actionReportSecurityProtocol = drkafkaConf.getActionReportProducerSecurityProtocol();

    LOG.info("Start rebuilding the replica stats by reading the past 24 hours brokerstats");
    ReplicaStatsManager.readPastReplicaStats(brokerstatsZkurl, statsSecurityProtocol,
        drkafkaConf.getBrokerStatsTopic(), drkafkaConf.getBrokerStatsBacktrackWindowsInSeconds());
    LOG.info("Finish rebuilding the replica stats");

    brokerStatsProcessor = new BrokerStatsProcessor(brokerstatsZkurl, statsSecurityProtocol, statsTopic,
        drkafkaConf.getBrokerStatsConsumerSslConfigs());
    brokerStatsProcessor.start();

    actionReporter = new DoctorKafkaActionReporter(actionReportZkurl, actionReportSecurityProtocol, actionReportTopic,
        drkafkaConf.getActionReportProducerSslConfigs());
    for (String clusterZkUrl : clusterZkUrls) {
      DoctorKafkaClusterConfig clusterConf = drkafkaConf.getClusterConfigByZkUrl(clusterZkUrl);
      KafkaCluster kafkaCluster = ReplicaStatsManager.clusters.get(clusterZkUrl);

      if (kafkaCluster == null) {
        LOG.error("No brokerstats info for cluster {}", clusterZkUrl);
        continue;
      }
      KafkaClusterManager clusterManager = new KafkaClusterManager(
          clusterZkUrl, kafkaCluster, clusterConf, drkafkaConf, actionReporter, zookeeperClient);
      clusterManagers.add(clusterManager);
      clusterManager.start();
      LOG.info("Starting cluster manager for " + clusterZkUrl);
    }
  }

  public void stop() {
    brokerStatsProcessor.stop();
    zookeeperClient.close();
    for (KafkaClusterManager clusterManager : clusterManagers) {
      clusterManager.stop();
    }
  }

  public DoctorKafkaConfig getDoctorKafkaConfig() {
    return drkafkaConf;
  }

  public List<KafkaClusterManager> getClusterManagers() {
    return clusterManagers;
  }

  public KafkaClusterManager getClusterManager(String clusterName) {
    for (KafkaClusterManager clusterManager : clusterManagers) {
      if (clusterManager.getClusterName().equals(clusterName)) {
        return clusterManager;
      }
    }
    return null;
  }
}
