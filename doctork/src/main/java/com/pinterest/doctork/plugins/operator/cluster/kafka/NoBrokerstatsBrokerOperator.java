package com.pinterest.doctork.plugins.operator.cluster.kafka;

import com.pinterest.doctork.plugins.context.state.cluster.kafka.KafkaState;
import com.pinterest.doctork.plugins.task.Task;
import com.pinterest.doctork.plugins.task.cluster.NotificationTask;

import kafka.cluster.Broker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * This operator detects brokers that haven't published brokerstats and sends notifications
 *
 * <pre>
 * Output Tasks Format:
 * Task: alert_no_brokerstats_brokers:
 * triggered when there are brokers that haven't published brokerstats
 * {
 *   title: str,
 *   message: str,
 * }
 * </pre>
 */

public class NoBrokerstatsBrokerOperator extends KafkaOperator {
  private static final Logger LOG = LogManager.getLogger(NoBrokerstatsBrokerOperator.class);
  private static final String TASK_ALERT_NO_BROKERSTATS_BROKERS_NAME = "alert_no_brokerstats_brokers";


  @Override
  public boolean operate(KafkaState state) throws Exception {
    List<Broker> noStatsBrokers = state.getNoBrokerstatsBrokers();
    if(noStatsBrokers != null && noStatsBrokers.size() > 0){
      try {
        emit(createNoBrokerstatsBrokerAlertTask(state.getClusterName(), state.getNoBrokerstatsBrokers()));
      } catch (Exception e){
        LOG.error("Failed to emit alert on no stats brokers task", e);
      }
      return false;
    }
    return true;
  }

  protected Task createNoBrokerstatsBrokerAlertTask(String clusterName, List<Broker> noStatsBrokers){
    String title = clusterName + " : " + noStatsBrokers.size() + " brokers do not have stats";
    StringBuilder msg = new StringBuilder();
    msg.append("No stats brokers : \n");
    noStatsBrokers.stream().forEach(broker -> msg.append(broker + "\n"));
    return new NotificationTask(TASK_ALERT_NO_BROKERSTATS_BROKERS_NAME, title, msg.toString());
  }
}
