package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.modules.manager.DoctorKafkaModuleManager;
import com.pinterest.doctorkafka.modules.manager.ModuleManager;
import com.pinterest.doctorkafka.replicastats.BrokerStatsProcessor;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DoctorKafka {
  private static final Logger LOG = LogManager.getLogger(DoctorKafka.class);

  private DoctorKafkaConfig drkafkaConf;
  private BrokerStatsProcessor brokerStatsProcessor = null;
  private Map<String, KafkaClusterManager> clusterManagers = new HashMap<>();
  private Set<String> clusterZkUrls = null;
  private ZookeeperClient zookeeperClient = null;
  private DoctorKafkaHeartbeat heartbeat = null;
  private ReplicaStatsManager replicaStatsManager = null;
  private ModuleManager moduleManager;

  public DoctorKafka(ReplicaStatsManager replicaStatsManager) throws Exception{
    this.replicaStatsManager = replicaStatsManager;
    this.drkafkaConf = replicaStatsManager.getConfig();
    this.clusterZkUrls = drkafkaConf.getClusterZkUrls();
    this.zookeeperClient = new ZookeeperClient(drkafkaConf.getDoctorKafkaZkurl());
    this.moduleManager = new DoctorKafkaModuleManager(
        drkafkaConf.getMonitorsConfiguration(),
        drkafkaConf.getOperatorsConfiguration(),
        drkafkaConf.getActionsConfiguration()
    );
  }

  public void start() throws Exception {
    String brokerstatsZkurl = drkafkaConf.getBrokerstatsZkurl();
    String statsTopic = drkafkaConf.getBrokerStatsTopic();
    SecurityProtocol statsSecurityProtocol = drkafkaConf.getBrokerStatsConsumerSecurityProtocol();

    LOG.info("Start rebuilding the replica stats by reading the past 24 hours brokerstats");
    replicaStatsManager.readPastReplicaStats(brokerstatsZkurl, statsSecurityProtocol,
        drkafkaConf.getBrokerStatsTopic(), drkafkaConf.getBrokerStatsBacktrackWindowsInSeconds());
    LOG.info("Finish rebuilding the replica stats");

    brokerStatsProcessor = new BrokerStatsProcessor(brokerstatsZkurl, statsSecurityProtocol, statsTopic,
        drkafkaConf.getBrokerStatsConsumerSslConfigs(), replicaStatsManager);
    brokerStatsProcessor.start();

    for (String clusterZkUrl : clusterZkUrls) {
      DoctorKafkaClusterConfig clusterConf = drkafkaConf.getClusterConfigByZkUrl(clusterZkUrl);
      KafkaCluster kafkaCluster = replicaStatsManager.getClusters().get(clusterZkUrl);

      if (kafkaCluster == null) {
        LOG.error("No brokerstats info for cluster {}", clusterZkUrl);
        continue;
      }
      KafkaClusterManager clusterManager = new KafkaClusterManager(
          clusterZkUrl, kafkaCluster, clusterConf, drkafkaConf, zookeeperClient, moduleManager);
      clusterManagers.put(clusterConf.getClusterName(), clusterManager);
      clusterManager.start();
      LOG.info("Starting cluster manager for " + clusterZkUrl);
    }

    heartbeat = new DoctorKafkaHeartbeat();
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

  public DoctorKafkaConfig getDoctorKafkaConfig() {
    return drkafkaConf;
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
