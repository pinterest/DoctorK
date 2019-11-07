package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.plugins.manager.DoctorKafkaPluginManager;
import com.pinterest.doctorkafka.plugins.manager.PluginManager;
import com.pinterest.doctorkafka.plugins.task.TaskDispatcher;
import com.pinterest.doctorkafka.plugins.task.TaskEmitter;
import com.pinterest.doctorkafka.util.ZookeeperClient;

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
  private Map<String, KafkaClusterManager> clusterManagers = new HashMap<>();
  private Set<String> clusterZkUrls = null;
  private ZookeeperClient zookeeperClient = null;
  private DoctorKafkaHeartbeat heartbeat = null;
  private PluginManager pluginManager;
  private Class<? extends TaskEmitter> taskEmitterClass;
  private Class<? extends TaskDispatcher> taskDispatcherClass;

  public DoctorKafka(DoctorKafkaConfig drkafkaConf) throws Exception{
    this.drkafkaConf = drkafkaConf;
    this.clusterZkUrls = drkafkaConf.getClusterZkUrls();
    this.zookeeperClient = new ZookeeperClient(drkafkaConf.getDoctorKafkaZkurl());
    this.pluginManager = new DoctorKafkaPluginManager();

    this.taskEmitterClass = Class.forName(drkafkaConf.getTaskEmitterClassName()).asSubclass(TaskEmitter.class);
    this.taskDispatcherClass = Class.forName(drkafkaConf.getTaskDispatcherClassName()).asSubclass(
        TaskDispatcher.class);
  }

  public void start() throws Exception {

    boolean isEmitterEqualToDispatcher = taskEmitterClass.equals(taskDispatcherClass);
    for (String clusterZkUrl : clusterZkUrls) {
      TaskEmitter emitter;
      TaskDispatcher dispatcher;
      emitter = taskEmitterClass.newInstance();

      if(isEmitterEqualToDispatcher){
        dispatcher = (TaskDispatcher) emitter;
      } else {
        dispatcher = taskDispatcherClass.newInstance();
      }

      DoctorKafkaClusterConfig clusterConf = drkafkaConf.getClusterConfigByZkUrl(clusterZkUrl);
      KafkaClusterManager clusterManager = new KafkaClusterManager(
          clusterZkUrl, clusterConf, drkafkaConf, zookeeperClient, pluginManager, emitter, dispatcher);
      clusterManagers.put(clusterConf.getClusterName(), clusterManager);
      clusterManager.start();
      LOG.info("Starting cluster manager for " + clusterZkUrl);
    }

    heartbeat = new DoctorKafkaHeartbeat();
    heartbeat.start();
  }

  public void stop() {
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
