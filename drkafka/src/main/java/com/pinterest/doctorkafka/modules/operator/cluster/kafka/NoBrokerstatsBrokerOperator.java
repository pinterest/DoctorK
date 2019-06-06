package com.pinterest.doctorkafka.modules.operator.cluster.kafka;

import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.event.Event;
import com.pinterest.doctorkafka.modules.event.NotificationEvent;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

import kafka.cluster.Broker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class NoBrokerstatsBrokerOperator extends KafkaOperator {
  private static final Logger LOG = LogManager.getLogger(NoBrokerstatsBrokerOperator.class);
  private static final String EVENT_ALERT_NO_BROKERSTATS_BROKERS_NAME = "alert_no_brokerstats_brokers";


  @Override
  public boolean operate(KafkaContext ctx, KafkaState state) throws Exception {
    List<Broker> noStatsBrokers = state.getNoBrokerstatsBrokers();
    if(noStatsBrokers != null && noStatsBrokers.size() > 0){
      try {
        emit(createNoBrokerstatsBrokerAlertEvent(ctx.getClusterName(), state.getNoBrokerstatsBrokers()));
      } catch (Exception e){
        LOG.error("Failed to emit alert on no stats brokers event", e);
      }
      return false;
    }
    return true;
  }

  protected Event createNoBrokerstatsBrokerAlertEvent(String clusterName, List<Broker> noStatsBrokers){
    String title = clusterName + " : " + noStatsBrokers.size() + " brokers do not have stats";
    StringBuilder msg = new StringBuilder();
    msg.append("No stats brokers : \n");
    noStatsBrokers.stream().forEach(broker -> msg.append(broker + "\n"));
    return new NotificationEvent(EVENT_ALERT_NO_BROKERSTATS_BROKERS_NAME, title, msg.toString());
  }
}
