package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.context.event.NotificationEvent;
import com.pinterest.doctorkafka.modules.context.event.SingleThreadEventHandler;
import com.pinterest.doctorkafka.modules.manager.ModuleManager;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;
import com.pinterest.doctorkafka.modules.context.state.State;
import com.pinterest.doctorkafka.modules.context.state.cluster.kafka.KafkaState;
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

public class KafkaClusterManager implements Runnable {

  private static final Logger LOG = LogManager.getLogger(KafkaClusterManager.class);
  private static final String EVENT_NOTIFY_MAINTENANCE_MODE_NAME = "notify_maintenance_mode";
  private static final String EVENT_NOTIFY_DECOMMISSION_NAME = "notify_decommission";

  KafkaState baseState = new KafkaState();
  private volatile KafkaState currentState = new KafkaState();
  private Collection<Monitor> monitors = new ArrayList<>();
  private Collection<Operator> operators = new ArrayList<>();
  private SingleThreadEventHandler eventHandler;

  private boolean stopped = false;
  private Thread thread = null;

  private long evaluationFrequency;

  public KafkaClusterManager(String zkUrl,
                             DoctorKafkaClusterConfig clusterConfig,
                             DoctorKafkaConfig drkafkaConfig,
                             ZookeeperClient zookeeperClient,
                             ModuleManager moduleManager) throws Exception {
    assert clusterConfig != null;
    baseState.setZkUrl(zkUrl);
    baseState.setZkUtils(KafkaUtils.getZkUtils(zkUrl));
    baseState.setClusterName(clusterConfig.getClusterName());
    baseState.setKafkaClusterZookeeperClient(zookeeperClient);

    evaluationFrequency = drkafkaConfig.getEvaluationFrequency();

    // Load monitor plugins
    Map<String, Configuration> baseMonitorsConfigs = drkafkaConfig.getMonitorsConfigs();
    for(Map.Entry<String, AbstractConfiguration> entry: clusterConfig.getEnabledMonitorsConfigs(baseMonitorsConfigs).entrySet() ){
      String monitorName = entry.getKey();
      AbstractConfiguration monitorConfig = entry.getValue();
      try {
        this.monitors.add(moduleManager.getMonitor(monitorConfig));
      } catch (ClassCastException e){
        LOG.error("Module {} is not a monitor module", monitorName, e);
        throw e;
      }
    }

    // create event handler
    this.eventHandler = new SingleThreadEventHandler();

    Map<String, Configuration> baseOperatorsConfigs = drkafkaConfig.getOperatorsConfigs();
    for(Map.Entry<String, AbstractConfiguration> entry: clusterConfig.getEnabledOperatorsConfigs(baseOperatorsConfigs).entrySet() ){
      String operatorName = entry.getKey();
      AbstractConfiguration operatorConfig = entry.getValue();
      try {
        Operator operator = moduleManager.getOperator(operatorConfig);
        operator.setEventEmitter(eventHandler);
        this.operators.add(operator);
      } catch (ClassCastException e){
        LOG.error("Module {} is not a operator module", operatorName, e);
        throw e;
      }
    }

    Map<String, Configuration> baseActionsConfigs = drkafkaConfig.getActionsConfigs();
    for(Map.Entry<String, AbstractConfiguration> entry: clusterConfig.getEnabledActionsConfigs(baseActionsConfigs).entrySet()){
      String actionName = entry.getKey();
      AbstractConfiguration actionConfig = entry.getValue();
      try {
        Action action = moduleManager.getAction(actionConfig);
        if (action.getSubscribedEvents().length == 0){
          LOG.warn("Action {} is not subscribing to any event.");
          continue;
        }
        for (String eventName : action.getSubscribedEvents()){
          eventHandler.subscribe(eventName, action);
        }
      } catch (ClassCastException e){
        LOG.error("Module {} is not a action module", actionName, e);
        throw e;
      }
    }
  }

  public KafkaCluster getCluster() {
    return currentState.getKafkaCluster();
  }

  public void start() {
    eventHandler.start();
    thread = new Thread(this);
    thread.setName("ClusterManager:" + getClusterName());
    thread.start();
  }

  public void stop() {
    eventHandler.stop();
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

  public void enableMaintenanceMode() {
    baseState.setUnderMaintenance(true);
    LOG.info("Enabled maintenace mode for:" + baseState.getClusterName());
    try {
      eventHandler.emit(new NotificationEvent(
          EVENT_NOTIFY_MAINTENANCE_MODE_NAME,
          getClusterName() + " is in maintenance mode",
          getClusterName() + " is placed in maintenance mode on " + new Date()
      ));
    } catch (Exception e){
      LOG.error("Failed to emit enable maintenance mode notification event.");
    }
  }

  public void disableMaintenanceMode() {
    baseState.setUnderMaintenance(false);
    LOG.info("Disabled maintenace mode for:" + getClusterName());
    try {
      eventHandler.emit(new NotificationEvent(
          EVENT_NOTIFY_MAINTENANCE_MODE_NAME,
          getClusterName() + " is out of maintenance mode",
          getClusterName() + " is removed from maintenance mode on " + new Date()
      ));
    } catch (Exception e) {
      LOG.error("Failed to emit disable maintenance mode notification event.");
    }
  }

  public void decommissionBroker(Integer brokerId) {
    boolean prevState = currentState.getKafkaCluster().getBroker(brokerId).decommission();

    // only notify if state changed
    if (prevState == false) {
      try {
        eventHandler.emit(new NotificationEvent(
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
    boolean prevState = currentState.getKafkaCluster().getBroker(brokerId).cancelDecommission();

    // only notify if state changed
    if (prevState == true) {
      try {
        eventHandler.emit(new NotificationEvent(
            EVENT_NOTIFY_DECOMMISSION_NAME,
            "Cancelled decommissioning broker " + brokerId + " on " + currentState.getClusterName(),
            "Broker:" + brokerId + " Cluster:" + currentState.getClusterName() + " decommission cancelled"
        ));
      } catch (Exception e) {
        LOG.error("Failed to emit cancelled broker decommissioning notification event.");
      }
    }
  }

  /**
   *  KafkaClusterManager periodically check the health of the cluster. If it finds
   *  an under-replicated partitions, it will perform partition reassignment. It will also
   *  do partition reassignment for workload balancing.
   *
   *  If partitions are under-replicated in the middle of work-load balancing due to
   *  broker failure, it will send out an alert. Human intervention is needed in this case.
   */
  @Override
  public void run() {

    while(!stopped) {
      State newState = deepCloneBaseState();
      try {
        Thread.sleep(evaluationFrequency);
      } catch (InterruptedException e){
        LOG.error("KafkaClusterManager interrupted, stopping evaluation loop...");
        stop();
        break;
      }
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

    return newState;
  }
}
