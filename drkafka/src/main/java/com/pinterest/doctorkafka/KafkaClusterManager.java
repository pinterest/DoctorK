package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.plugins.action.Action;
import com.pinterest.doctorkafka.plugins.context.event.EventDispatcher;
import com.pinterest.doctorkafka.plugins.context.event.EventEmitter;
import com.pinterest.doctorkafka.plugins.context.event.NotificationEvent;
import com.pinterest.doctorkafka.plugins.context.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.plugins.manager.PluginManager;
import com.pinterest.doctorkafka.plugins.monitor.Monitor;
import com.pinterest.doctorkafka.plugins.operator.Operator;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import kafka.cluster.Broker;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * KafkaClusterManager periodically loops through the {@link Monitor Monitors} and {@link Operator Operators}
 * defined in the configuration file.
 */

public class KafkaClusterManager implements Runnable {

  private static final Logger LOG = LogManager.getLogger(KafkaClusterManager.class);
  private static final String EVENT_NOTIFY_MAINTENANCE_MODE_NAME = "notify_maintenance_mode";
  private static final String EVENT_NOTIFY_DECOMMISSION_NAME = "notify_decommission";

  private volatile KafkaState state = new KafkaState();
  private Collection<Monitor> monitors = new ArrayList<>();
  private Collection<Operator> operators = new ArrayList<>();
  private EventEmitter eventEmitter;
  private EventDispatcher eventDispatcher;

  private boolean stopped = false;
  private Thread thread = null;

  private long evaluationFrequency;

  public KafkaClusterManager(String zkUrl,
                             DoctorKafkaClusterConfig clusterConfig,
                             DoctorKafkaConfig drkafkaConfig,
                             ZookeeperClient zookeeperClient,
                             PluginManager pluginManager,
                             EventEmitter eventEmitter,
                             EventDispatcher eventDispatcher) throws Exception {
    assert clusterConfig != null;

    /* baseState is a State that keeps configurations and manual settings from the UI/API,
       it is cloned by the manager each time the evaluation loop begins
    */

    state.setZkUrl(zkUrl);
    state.setClusterName(clusterConfig.getClusterName());
    state.setKafkaClusterZookeeperClient(zookeeperClient);

    evaluationFrequency = drkafkaConfig.getEvaluationFrequency();

    this.eventEmitter = eventEmitter;
    this.eventDispatcher = eventDispatcher;

    loadPlugins(drkafkaConfig, clusterConfig, pluginManager);

  }

  public KafkaState getState() {
    return state;
  }

  public KafkaCluster getCluster() {
    return state.getKafkaCluster();
  }

  public void start() {
    eventDispatcher.start();
    thread = new Thread(this);
    thread.setName("ClusterManager:" + getClusterName());
    thread.start();
  }

  public void stop() {
    eventDispatcher.stop();
    stopped = true;
  }

  public String getClusterName() {
    return state.getClusterName();
  }

  public int getClusterSize() {
    KafkaCluster kafkaCluster = state.getKafkaCluster();
    if (kafkaCluster == null) {
      LOG.error("kafkaCluster is null for {}", state.getZkUrl());
    }
    return kafkaCluster.size();
  }

  public List<PartitionInfo> getUnderReplicatedPartitions() {
    return state.getUnderReplicatedPartitions();
  }

  /**
   *   return the list of brokers that do not have stats
   */
  public List<Broker> getNoStatsBrokers() {
    return state.getNoBrokerstatsBrokers();
  }

  public List<KafkaBroker> getAllBrokers() {
    return new ArrayList<>(state.getKafkaCluster().brokers.values());
  }
  
  public KafkaState getCurrentState() {
    return state;
  }
  
  public void enableMaintenanceMode() {
    state.setUnderMaintenance(true);
    LOG.info("Enabled maintenace mode for:" + state.getClusterName());
    try {
      eventEmitter.emit(new NotificationEvent(
          EVENT_NOTIFY_MAINTENANCE_MODE_NAME,
          getClusterName() + " is in maintenance mode",
          getClusterName() + " is placed in maintenance mode on " + new Date()
      ));
    } catch (Exception e){
      LOG.error("Failed to emit enable maintenance mode notification event.");
    }
  }

  public void disableMaintenanceMode() {
    state.setUnderMaintenance(false);
    LOG.info("Disabled maintenace mode for:" + getClusterName());
    try {
      eventEmitter.emit(new NotificationEvent(
          EVENT_NOTIFY_MAINTENANCE_MODE_NAME,
          getClusterName() + " is out of maintenance mode",
          getClusterName() + " is removed from maintenance mode on " + new Date()
      ));
    } catch (Exception e) {
      LOG.error("Failed to emit disable maintenance mode notification event.");
    }
  }

  public void decommissionBroker(Integer brokerId) {
    boolean prevState = state.getKafkaCluster().getBroker(brokerId).decommission();

    // only notify if state changed
    if (prevState == false) {
      try {
        eventEmitter.emit(new NotificationEvent(
            EVENT_NOTIFY_DECOMMISSION_NAME,
            "Decommissioning broker " + brokerId + " on " + getClusterName(),
            "Broker:" + brokerId + " Cluster:" + getClusterName()+ " is getting decommissioned"
        ));
      } catch (Exception e) {
        LOG.error("Failed to emit broker decommission notification event.");
      }
    }
  }

  public void cancelDecommissionBroker(Integer brokerId) {
    boolean prevState = state.getKafkaCluster().getBroker(brokerId).cancelDecommission();

    // only notify if state changed
    if (prevState == true) {
      try {
        eventEmitter.emit(new NotificationEvent(
            EVENT_NOTIFY_DECOMMISSION_NAME,
            "Cancelled decommissioning broker " + brokerId + " on " + state.getClusterName(),
            "Broker:" + brokerId + " Cluster:" + state.getClusterName() + " decommission cancelled"
        ));
      } catch (Exception e) {
        LOG.error("Failed to emit cancelled broker decommissioning notification event.");
      }
    }
  }

  @Override
  public void run() {

    // This is the evaluation loop of the cluster. It first waits for a period of time,
    // after the sleep, it goes through all the monitor plugins first and then the operators
    // the plugins are evaluated based on the ordering in the configurations
    while(!stopped) {
      try {
        Thread.sleep(evaluationFrequency);
      } catch (InterruptedException e){
        LOG.error("KafkaClusterManager interrupted, stopping evaluation loop...");
        stop();
        break;
      }
      for (Monitor plugin: monitors) {
        if (state.isOperationsStopped()){
          break;
        }

        try{
          plugin.observe(state);
        } catch (Exception e) {
          LOG.error("Error when evaluating monitor: {}", plugin.getClass(), e);
        }
      }

      // short circuit if stopped
      if (state.isOperationsStopped()){
        continue;
      }

      for (Operator operator: operators) {
        try {
          if(!operator.operate(state)){
            break;
          }
        } catch (Exception e){
          LOG.error("Failed when performing operation {}:", operator.getClass(), e);
        }
      }
    }
  }

  public boolean isMaintenanceModeEnabled() {
    return state.isUnderMaintenance();
  }

  protected void loadPlugins(DoctorKafkaConfig drkafkaConfig, DoctorKafkaClusterConfig clusterConfig, PluginManager pluginManager) throws Exception {
    // load plugins
    loadMonitorPlugins(drkafkaConfig, clusterConfig, pluginManager);
    loadOperatorPlugins(drkafkaConfig, clusterConfig, pluginManager);
    loadActionPlugins(drkafkaConfig, clusterConfig, pluginManager);
  }

  protected void loadMonitorPlugins(DoctorKafkaConfig drkafkaConfig, DoctorKafkaClusterConfig clusterConfig, PluginManager pluginManager) throws Exception{
    Map<String, Configuration> baseMonitorsConfigs = drkafkaConfig.getMonitorsConfigs();
    for(Map.Entry<String, AbstractConfiguration> entry: clusterConfig.getEnabledMonitorsConfigs(baseMonitorsConfigs).entrySet() ){
      String monitorName = entry.getKey();
      AbstractConfiguration monitorConfig = entry.getValue();
      try {
        this.monitors.add(pluginManager.getMonitor(monitorConfig));
      } catch (ClassCastException e){
        LOG.error("Plugin {} is not a monitor plugin", monitorName, e);
        throw e;
      }
    }
  };

  protected void loadOperatorPlugins(DoctorKafkaConfig drkafkaConfig, DoctorKafkaClusterConfig clusterConfig, PluginManager pluginManager) throws Exception{
    Map<String, Configuration> baseOperatorsConfigs = drkafkaConfig.getOperatorsConfigs();
    for(Map.Entry<String, AbstractConfiguration> entry: clusterConfig.getEnabledOperatorsConfigs(baseOperatorsConfigs).entrySet() ){
      String operatorName = entry.getKey();
      AbstractConfiguration operatorConfig = entry.getValue();
      try {
        Operator operator = pluginManager.getOperator(operatorConfig);
        operator.setEventEmitter(eventEmitter);
        this.operators.add(operator);
      } catch (ClassCastException e){
        LOG.error("Plugin {} is not a operator plugin", operatorName, e);
        throw e;
      }
    }
  };

  protected void loadActionPlugins(DoctorKafkaConfig drkafkaConfig, DoctorKafkaClusterConfig clusterConfig, PluginManager pluginManager) throws Exception{
    Map<String, Configuration> baseActionsConfigs = drkafkaConfig.getActionsConfigs();
    for(Map.Entry<String, AbstractConfiguration> entry: clusterConfig.getEnabledActionsConfigs(baseActionsConfigs).entrySet()){
      String actionName = entry.getKey();
      AbstractConfiguration actionConfig = entry.getValue();
      try {
        Action action = pluginManager.getAction(actionConfig);
        if (action.getSubscribedEvents().length == 0){
          LOG.warn("Action {} is not subscribing to any event.", actionName);
          continue;
        }
        for (String eventName : action.getSubscribedEvents()){
          eventDispatcher.subscribe(eventName, action);
        }
      } catch (ClassCastException e){
        LOG.error("Plugin {} is not a action plugin", actionName, e);
        throw e;
      }
    }
  }
}
