package com.pinterest.doctorkafka;

import com.pinterest.doctorkafka.config.DoctorKafkaClusterConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.modules.action.ReportOperation;
import com.pinterest.doctorkafka.modules.action.SendEvent;
import com.pinterest.doctorkafka.modules.action.cluster.ReplaceInstance;
import com.pinterest.doctorkafka.modules.action.cluster.kafka.ReassignPartition;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.manager.ActionManager;
import com.pinterest.doctorkafka.modules.manager.DoctorKafkaActionManager;
import com.pinterest.doctorkafka.modules.manager.MonitorManager;
import com.pinterest.doctorkafka.modules.manager.OperatorManager;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;
import com.pinterest.doctorkafka.modules.operator.cluster.kafka.BrokerReplacer;
import com.pinterest.doctorkafka.modules.operator.cluster.kafka.NoStatsBrokersOperator;
import com.pinterest.doctorkafka.modules.operator.cluster.kafka.URPReassignor;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import kafka.cluster.Broker;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;


  /**
   *  There are primarily three reasons for partition under-replication:
   *    1. network saturation on leader broker
   *    2. dead broker
   *    3. degraded hardware
   */
public class KafkaClusterManager implements Runnable {

  private static final Logger
      LOG = LogManager.getLogger(KafkaClusterManager.class);

  /**
   *  The number of broker stats that we need to examine to tell if a broker dies or not.
   */

  private KafkaContext ctx;
  private KafkaState currentState;
  private Collection<Monitor> monitors = new ArrayList<>();
  private Operator noStatsBrokersOperator;
  private Operator brokerReplacer;
  private Operator urpReassignor;

  private SendEvent sendEvent;

  private boolean stopped = false;
  private Thread thread = null;

  public KafkaClusterManager(String zkUrl, KafkaCluster kafkaCluster,
                             DoctorKafkaClusterConfig clusterConfig,
                             DoctorKafkaConfig drkafkaConfig,
                             ZookeeperClient zookeeperClient,
                             MonitorManager monitorManager,
                             ActionManager actionManager,
                             OperatorManager operatorManager) throws Exception {
    assert clusterConfig != null;
    ctx = new KafkaContext();
    ctx.setKafkaCluster(kafkaCluster);
    ctx.setZkUrl(zkUrl);
    ctx.setZkUtils(KafkaUtils.getZkUtils(zkUrl));
    ctx.setClusterName(clusterConfig.getClusterName());
    ctx.setKafkaClusterZookeeperClient(zookeeperClient);

    // try to get cluster-specific monitor plugins, if null, get top-level default plugins
    String[] monitors;
    monitors = clusterConfig.getEnabledMonitors();
    if (monitors == null) {
      monitors = drkafkaConfig.getEnabledMonitors();
    }

    // Load monitor plugins
    for(String monitor : monitors){
      this.monitors.add(monitorManager.getMonitor(
          monitor,
          clusterConfig.getMonitorConfiguration(monitor)
      ));
    }

    // Load actions and inject to operators
    SendEvent sendEvent = actionManager.getSendEvent(
        clusterConfig.getActionConfiguration(DoctorKafkaActionManager.ACTION_SEND_EVENT_NAME)
    );
    ReportOperation reportOperation = actionManager.getReportOperation(
        clusterConfig.getActionConfiguration(DoctorKafkaActionManager.ACTION_REPORT_OPERATION_NAME)
    );
    ReplaceInstance replaceInstance = actionManager.getReplaceInstance(
        clusterConfig.getActionConfiguration(DoctorKafkaActionManager.ACTION_REPLACE_INSTANCE_NAME)
    );
    ReassignPartition reassignPartition = actionManager.getReassignPartition(
        clusterConfig.getActionConfiguration(DoctorKafkaActionManager.ACTION_REASSIGN_PARTITION_NAME)
    );

    this.sendEvent = sendEvent;

    noStatsBrokersOperator = new NoStatsBrokersOperator(sendEvent);
    brokerReplacer = new BrokerReplacer(sendEvent, replaceInstance, reportOperation);
    urpReassignor = new URPReassignor(sendEvent, reassignPartition, reportOperation);

    Operator[] operators = new Operator[]{
        noStatsBrokersOperator,
        brokerReplacer,
        urpReassignor
    };

    // Configure operators
    for(Operator operator : operators){
      Configuration clusterOperatorConfig = clusterConfig.getOperatorConfiguration(operator.getConfigName());
      operatorManager.configureOperator(operator, clusterOperatorConfig);
    }

  }

  public KafkaCluster getCluster() {
    return ctx.getKafkaCluster();
  }

  public void start() {
    thread = new Thread(this);
    thread.setName("ClusterManager:" + getClusterName());
    thread.start();
  }

  public void stop() {
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
    return currentState.getNoStatsBrokers();
  }

  public List<KafkaBroker> getAllBrokers() {
    return new ArrayList<>(ctx.getKafkaCluster().brokers.values());
  }

  public void enableMaintenanceMode() {
    ctx.setUnderMaintenance(true);
    LOG.info("Enabled maintenace mode for:" + ctx.getClusterName());
    try {
      sendEvent.notify(
          ctx.getClusterName() + " is in maintenance mode",
          ctx.getClusterName() + " is placed in maintenance mode on " + new Date());
    } catch (Exception e){
      LOG.error("Failed to notify enable of maintenance mode event.");
    }
  }

  public void disableMaintenanceMode() {
    ctx.setUnderMaintenance(false);
    LOG.info("Disabled maintenace mode for:" + ctx.getClusterName());
    try {
      sendEvent.notify(
          ctx.getClusterName() + " is out of maintenance mode",
          ctx.getClusterName() + " is removed from maintenance mode on " + new Date());
    } catch (Exception e) {
      LOG.error("Failed to notify disable of maintenance mode event.");
    }
  }

  public void decommissionBroker(Integer brokerId) {
    boolean prevState = ctx.getKafkaCluster().getBroker(brokerId).decommission();

    // only notify if state changed
    if (prevState == false) {
      try {
        sendEvent.notify("Decommissioning broker " + brokerId + " on " + ctx.getClusterName(),
            "Broker:" + brokerId + " Cluster:" + ctx.getClusterName()+ " is getting decommissioned");
      } catch (Exception e) {
        LOG.error("Failed to notify broker decommission event.");
      }
    }
  }

  public void cancelDecommissionBroker(Integer brokerId) {
    boolean prevState = ctx.getKafkaCluster().getBroker(brokerId).cancelDecommission();

    // only notify if state changed
    if (prevState == true) {
      try {
        sendEvent.notify("Cancelled decommissioning broker " + brokerId + " on " + ctx.getClusterName(),
            "Broker:" + brokerId + " Cluster:" + ctx.getClusterName() + " decommission cancelled");
      } catch (Exception e) {
        LOG.error("Failed to notify cancelled broker decommissioning event.");
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
      KafkaState newState = new KafkaState();

      try {
        Thread.sleep(5000L);
      } catch (InterruptedException e){

      }

      for (Monitor plugin: monitors) {
        if (newState.isStopped()){
          break;
        }
        try{
          plugin.observe(ctx, newState);
        } catch (Exception e) {
          LOG.error("Error when evaluating monitor: {}", plugin.getClass(), e);
        }
      }

      // short circuit if stopped
      if (!newState.isStopped()){
        continue;
      }

      currentState = newState;

      try {
        if(!noStatsBrokersOperator.operate(ctx, currentState)){
          continue;
        }
        urpReassignor.operate(ctx, currentState);
        brokerReplacer.operate(ctx, currentState);
      } catch (Exception e){
        LOG.error("Failed when performing operations:", e);
      }


    }
  }

  public boolean isMaintenanceModeEnabled() {
    return ctx.isUnderMaintenance();
  }
}
