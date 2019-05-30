package com.pinterest.doctorkafka.modules.operator.cluster.kafka;

import com.pinterest.doctorkafka.modules.action.SendEvent;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

import kafka.cluster.Broker;

import java.util.List;

public class NoStatsBrokersOperator extends KafkaOperator {
  private static final String CONFIG_NAME = "no_stats_brokers_operator";
  private SendEvent sendEvent;

  public NoStatsBrokersOperator(SendEvent sendEvent){
    this.sendEvent = sendEvent;
  }

  @Override
  public String getConfigName() {
    return CONFIG_NAME;
  }

  @Override
  public boolean operate(KafkaContext ctx, KafkaState state) throws Exception {
    List<Broker> noStatsBrokers = state.getNoStatsBrokers();
    if(noStatsBrokers.size() > 0){
      String title = ctx.getClusterName()+ " : " + noStatsBrokers.size() + " brokers do not have stats";
      StringBuilder sb = new StringBuilder();
      sb.append("No stats brokers : \n");
      noStatsBrokers.stream().forEach(broker -> sb.append(broker + "\n"));
      sendEvent.alert(title,sb.toString());
      return false;
    }
    return true;
  }
}
