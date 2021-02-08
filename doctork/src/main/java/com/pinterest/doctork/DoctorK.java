package com.pinterest.doctork;

import com.pinterest.doctork.config.DoctorKClusterConfig;
import com.pinterest.doctork.config.DoctorKConfig;
import com.pinterest.doctork.plugins.manager.DoctorKPluginManager;
import com.pinterest.doctork.plugins.manager.PluginManager;
import com.pinterest.doctork.plugins.task.TaskDispatcher;
import com.pinterest.doctork.plugins.task.TaskEmitter;
import com.pinterest.doctork.util.ZookeeperClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DoctorK {
  private static final Logger LOG = LogManager.getLogger(com.pinterest.doctork.DoctorK.class);

  private DoctorKConfig doctorkConf;
  private Map<String, KafkaClusterManager> clusterManagers = new HashMap<>();
  private Set<String> clusterZkUrls = null;
  private ZookeeperClient zookeeperClient = null;
  private DoctorKHeartbeat heartbeat = null;
  private PluginManager pluginManager;
  private Class<? extends TaskEmitter> taskEmitterClass;
  private Class<? extends TaskDispatcher> taskDispatcherClass;

  public DoctorK(DoctorKConfig doctorkConf) throws Exception{
    this.doctorkConf = doctorkConf;
    this.clusterZkUrls = doctorkConf.getClusterZkUrls();
    this.zookeeperClient = new ZookeeperClient(doctorkConf.getDoctorKZkurl());
    this.pluginManager = new DoctorKPluginManager();

    this.taskEmitterClass = Class.forName(doctorkConf.getTaskEmitterClassName()).asSubclass(TaskEmitter.class);
    this.taskDispatcherClass = Class.forName(doctorkConf.getTaskDispatcherClassName()).asSubclass(
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

      DoctorKClusterConfig clusterConf = doctorkConf.getClusterConfigByZkUrl(clusterZkUrl);
      KafkaClusterManager clusterManager = new KafkaClusterManager(
          clusterZkUrl, clusterConf, doctorkConf, zookeeperClient, pluginManager, emitter, dispatcher);
      clusterManagers.put(clusterConf.getClusterName(), clusterManager);
      clusterManager.start();
      LOG.info("Starting cluster manager for " + clusterZkUrl);
    }

    heartbeat = new DoctorKHeartbeat();
    heartbeat.start();
  }

  public void stop() {
    zookeeperClient.close();
    heartbeat.stop();
    for (KafkaClusterManager clusterManager : clusterManagers.values()) {
      clusterManager.stop();
    }
  }

  public DoctorKConfig getDoctorKConfig() {
    return doctorkConf;
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
