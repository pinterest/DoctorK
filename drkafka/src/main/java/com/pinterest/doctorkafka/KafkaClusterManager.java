package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.plugins.manager.PluginManager;
import com.pinterest.doctorkafka.plugins.monitor.Monitor;
import com.pinterest.doctorkafka.plugins.operator.Operator;
import com.pinterest.doctorkafka.plugins.task.TaskDispatcher;
import com.pinterest.doctorkafka.plugins.task.TaskEmitter;
import com.pinterest.doctorkafka.plugins.task.TaskHandler;
import com.pinterest.doctorkafka.plugins.task.cluster.NotificationTask;
import com.pinterest.doctorkafka.plugins.context.state.State;
import com.pinterest.doctorkafka.plugins.context.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.util.KafkaUtils;
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
  private static final String TASK_NOTIFY_MAINTENANCE_MODE_NAME = "notify_maintenance_mode";
  private static final String TASK_NOTIFY_DECOMMISSION_NAME = "notify_decommission";

  private KafkaState baseState = new KafkaState();
  private volatile KafkaState currentState = new KafkaState();
  private Collection<Monitor> monitors = new ArrayList<>();
  private Collection<Operator> operators = new ArrayList<>();
  private TaskEmitter taskEmitter;
  private TaskDispatcher taskDispatcher;

  private boolean stopped = false;
  private Thread thread = null;

  private long evaluationFrequency;

  public KafkaClusterManager(String zkUrl,
                             DoctorKafkaClusterConfig clusterConfig,
                             DoctorKafkaConfig drkafkaConfig,
                             ZookeeperClient zookeeperClient,
                             PluginManager pluginManager,
                             TaskEmitter taskEmitter,
                             TaskDispatcher taskDispatcher) throws Exception {
    assert clusterConfig != null;

    /* baseState is a State that keeps configurations and manual settings from the UI/API,
       it is cloned by the manager each time the evaluation loop begins
    */

    baseState.setZkUrl(zkUrl);
    baseState.setZkUtils(KafkaUtils.getZkUtils(zkUrl));
    baseState.setClusterName(clusterConfig.getClusterName());
    baseState.setKafkaClusterZookeeperClient(zookeeperClient);

    evaluationFrequency = drkafkaConfig.getEvaluationFrequency();

    this.taskEmitter = taskEmitter;
    this.taskDispatcher = taskDispatcher;

    loadPlugins(drkafkaConfig, clusterConfig, pluginManager);

  }

  public KafkaCluster getCluster() {
    return currentState.getKafkaCluster();
  }

  public void start() {
    taskDispatcher.start();
    thread = new Thread(this);
    thread.setName("ClusterManager:" + getClusterName());
    thread.start();
  }

  public void stop() {
    taskDispatcher.stop();
    stopped = true;
  }

  public String getClusterName() {
    return baseState.getClusterName();
  }

  public int getClusterSize() {
    KafkaCluster kafkaCluster = currentState.getKafkaCluster();
    if (kafkaCluster == null) {
      LOG.error("kafkaCluster is null for {}", currentState.getZkUrl());
    }
    return kafkaCluster.size();
  }

  public List<PartitionInfo> getUnderReplicatedPartitions() {
    return currentState.getUnderReplicatedPartitions();
  }

  /**
   *   return the list of brokers that do not have stats
   */
  public List<Broker> getNoStatsBrokers() {
    return currentState.getNoBrokerstatsBrokers();
  }

  public List<KafkaBroker> getAllBrokers() {
    return new ArrayList<>(currentState.getKafkaCluster().brokers.values());
  }
  
  public KafkaState getCurrentState() {
    return currentState;
  }
  
  public void enableMaintenanceMode() {
    baseState.setUnderMaintenance(true);
    LOG.info("Enabled maintenace mode for:" + baseState.getClusterName());
    try {
      taskEmitter.emit(new NotificationTask(
          TASK_NOTIFY_MAINTENANCE_MODE_NAME,
          getClusterName() + " is in maintenance mode",
          getClusterName() + " is placed in maintenance mode on " + new Date()
      ));
    } catch (Exception e){
      LOG.error("Failed to emit enable maintenance mode notification task.");
    }
  }

  public void disableMaintenanceMode() {
    baseState.setUnderMaintenance(false);
    LOG.info("Disabled maintenace mode for:" + getClusterName());
    try {
      taskEmitter.emit(new NotificationTask(
          TASK_NOTIFY_MAINTENANCE_MODE_NAME,
          getClusterName() + " is out of maintenance mode",
          getClusterName() + " is removed from maintenance mode on " + new Date()
      ));
    } catch (Exception e) {
      LOG.error("Failed to emit disable maintenance mode notification task.");
    }
  }

  public void decommissionBroker(Integer brokerId) {
    boolean prevState = currentState.getKafkaCluster().getBroker(brokerId).decommission();

    // only notify if state changed
    if (prevState == false) {
      try {
        taskEmitter.emit(new NotificationTask(
            TASK_NOTIFY_DECOMMISSION_NAME,
            "Decommissioning broker " + brokerId + " on " + getClusterName(),
            "Broker:" + brokerId + " Cluster:" + getClusterName()+ " is getting decommissioned"
        ));
      } catch (Exception e) {
        LOG.error("Failed to emit broker decommission notification task.");
      }
    }
  }

  public void cancelDecommissionBroker(Integer brokerId) {
    boolean prevState = currentState.getKafkaCluster().getBroker(brokerId).cancelDecommission();

    // only notify if state changed
    if (prevState == true) {
      try {
        taskEmitter.emit(new NotificationTask(
            TASK_NOTIFY_DECOMMISSION_NAME,
            "Cancelled decommissioning broker " + brokerId + " on " + currentState.getClusterName(),
            "Broker:" + brokerId + " Cluster:" + currentState.getClusterName() + " decommission cancelled"
        ));
      } catch (Exception e) {
        LOG.error("Failed to emit cancelled broker decommissioning notification task.");
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
      State newState = deepCloneBaseState();
      for (Monitor plugin: monitors) {
        if (newState.isOperationsStopped()){
          break;
        }
        try{
          newState = plugin.observe(newState);
        } catch (Exception e) {
          LOG.error("Error when evaluating monitor: {}", plugin.getClass(), e);
        }
      }

      // short circuit if stopped
      if (newState.isOperationsStopped()){
        continue;
      }

      currentState = (KafkaState) newState;

      for (Operator operator: operators) {
        try {
          if(!operator.operate(currentState)){
            break;
          }
        } catch (Exception e){
          LOG.error("Failed when performing operation {}:", operator.getClass(), e);
        }
      }
    }
  }

  public boolean isMaintenanceModeEnabled() {
    return baseState.isUnderMaintenance();
  }

  // utility function to create a deep clone of the base state object
  protected KafkaState deepCloneBaseState() {
    KafkaState newState = new KafkaState();
    newState.setUnderMaintenance(baseState.isUnderMaintenance());
    newState.setClusterName(baseState.getClusterName());
    newState.setZkUtils(baseState.getZkUtils());
    newState.setZkUrl(baseState.getZkUrl());
    newState.setKafkaClusterZookeeperClient(baseState.getKafkaClusterZookeeperClient());

    return newState;
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
        operator.setTaskEmitter(taskEmitter);
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
        TaskHandler action = pluginManager.getAction(actionConfig);
        if (action.getSubscribedTasks().length == 0){
          LOG.warn("Action {} is not subscribing to any task.", actionName);
          continue;
        }
        for (String taskName : action.getSubscribedTasks()){
          taskDispatcher.subscribe(taskName, action);
        }
      } catch (ClassCastException e){
        LOG.error("Plugin {} is not a action plugin", actionName, e);
        throw e;
      }
    }
  }
}
