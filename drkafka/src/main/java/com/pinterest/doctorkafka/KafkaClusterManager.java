package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.event.NotificationEvent;
import com.pinterest.doctorkafka.modules.event.SingleThreadEventHandler;
import com.pinterest.doctorkafka.modules.manager.ModuleManager;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;
import com.pinterest.doctorkafka.modules.state.State;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import kafka.cluster.Broker;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class KafkaClusterManager implements Runnable {

  private static final Logger LOG = LogManager.getLogger(KafkaClusterManager.class);
  private static final String EVENT_NOTIFY_MAINTENANCE_MODE_NAME = "notify_maintenance_mode";
  private static final String EVENT_NOTIFY_DECOMMISSION_NAME = "notify_decommission";

  private KafkaContext ctx;
  private volatile KafkaState currentState = new KafkaState();
  private Collection<Monitor> monitors = new ArrayList<>();
  private Collection<Operator> operators = new ArrayList<>();
  private SingleThreadEventHandler eventHandler;

  private boolean stopped = false;
  private Thread thread = null;

  public KafkaClusterManager(String zkUrl, KafkaCluster kafkaCluster,
                             DoctorKafkaClusterConfig clusterConfig,
                             DoctorKafkaConfig drkafkaConfig,
                             ZookeeperClient zookeeperClient,
                             ModuleManager moduleManager) throws Exception {
    assert clusterConfig != null;
    ctx = new KafkaContext();
    ctx.setKafkaCluster(kafkaCluster);
    ctx.setZkUrl(zkUrl);
    ctx.setZkUtils(KafkaUtils.getZkUtils(zkUrl));
    ctx.setClusterName(clusterConfig.getClusterName());
    ctx.setKafkaClusterZookeeperClient(zookeeperClient);

    // try to get cluster-specific monitor plugins, if null, get top-level default plugins
    String[] monitorNames = clusterConfig.getEnabledMonitors();
    if (monitorNames == null) {
      monitorNames = drkafkaConfig.getEnabledMonitors();
    }

    // Load monitor plugins
    for(String monitorName : monitorNames){
      try {
        this.monitors.add(moduleManager.getMonitor(
            monitorName,
            clusterConfig.getMonitorConfiguration(monitorName)
        ));
      } catch (ClassCastException e){
        LOG.error("Module {} is not a monitor module", monitorName, e);
        throw e;
      }
    }

    // create event handler
    this.eventHandler = new SingleThreadEventHandler();

    // Load operator plugins
    String[] operatorNames = clusterConfig.getEnabledOperators();
    if (operatorNames  == null) {
      operatorNames  = drkafkaConfig.getEnabledOperators();
    }

    for(String operatorName: operatorNames ){
      try {
        Operator operator = moduleManager.getOperator(
            operatorName,
            clusterConfig.getOperatorConfiguration(operatorName)
        );
        operator.setEventEmitter(eventHandler);
        this.operators.add(operator);
      } catch (ClassCastException e){
        LOG.error("Module {} is not a operator module", operatorName, e);
        throw e;
      }
    }

    String[] actionNames = clusterConfig.getEnabledActions();
    if (actionNames == null){
      actionNames = drkafkaConfig.getEnabledActions();
    }

    for (String actionName: actionNames){
      try {
        Action action = moduleManager.getAction(
            actionName,
            clusterConfig.getActionConfiguration(actionName)
        );
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
    return ctx.getKafkaCluster();
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
    return ctx.getClusterName();
  }

  public int getClusterSize() {
    KafkaCluster kafkaCluster = ctx.getKafkaCluster();
    if (kafkaCluster == null) {
      LOG.error("kafkaCluster is null for {}", ctx.getZkUrl());
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
    return new ArrayList<>(ctx.getKafkaCluster().brokers.values());
  }

  public void enableMaintenanceMode() {
    ctx.setUnderMaintenance(true);
    LOG.info("Enabled maintenace mode for:" + ctx.getClusterName());
    try {
      eventHandler.emit(new NotificationEvent(
          EVENT_NOTIFY_MAINTENANCE_MODE_NAME,
          ctx.getClusterName() + " is in maintenance mode",
          ctx.getClusterName() + " is placed in maintenance mode on " + new Date()
      ));
    } catch (Exception e){
      LOG.error("Failed to emit enable maintenance mode notification event.");
    }
  }

  public void disableMaintenanceMode() {
    ctx.setUnderMaintenance(false);
    LOG.info("Disabled maintenace mode for:" + ctx.getClusterName());
    try {
      eventHandler.emit(new NotificationEvent(
          EVENT_NOTIFY_MAINTENANCE_MODE_NAME,
          ctx.getClusterName() + " is out of maintenance mode",
          ctx.getClusterName() + " is removed from maintenance mode on " + new Date()
      ));
    } catch (Exception e) {
      LOG.error("Failed to emit disable maintenance mode notification event.");
    }
  }

  public void decommissionBroker(Integer brokerId) {
    boolean prevState = ctx.getKafkaCluster().getBroker(brokerId).decommission();

    // only notify if state changed
    if (prevState == false) {
      try {
        eventHandler.emit(new NotificationEvent(
            EVENT_NOTIFY_DECOMMISSION_NAME,
            "Decommissioning broker " + brokerId + " on " + ctx.getClusterName(),
            "Broker:" + brokerId + " Cluster:" + ctx.getClusterName()+ " is getting decommissioned"
        ));
      } catch (Exception e) {
        LOG.error("Failed to emit broker decommission notification event.");
      }
    }
  }

  public void cancelDecommissionBroker(Integer brokerId) {
    boolean prevState = ctx.getKafkaCluster().getBroker(brokerId).cancelDecommission();

    // only notify if state changed
    if (prevState == true) {
      try {
        eventHandler.emit(new NotificationEvent(
            EVENT_NOTIFY_DECOMMISSION_NAME,
            "Cancelled decommissioning broker " + brokerId + " on " + ctx.getClusterName(),
            "Broker:" + brokerId + " Cluster:" + ctx.getClusterName() + " decommission cancelled"
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
      State newState = new KafkaState();

      for (Monitor plugin: monitors) {
        if (newState.isOperationsStopped()){
          break;
        }
        try{
          newState = plugin.observe(ctx, newState);
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
          if(!operator.operate(ctx, currentState)){
            break;
          }
        } catch (Exception e){
          LOG.error("Failed when performing operation {}:", operator.getClass(), e);
        }
      }
    }
  }

  public boolean isMaintenanceModeEnabled() {
    return ctx.isUnderMaintenance();
  }
}
