package com.pinterest.doctorkafka.modules.monitor.cluster.kafka;

import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

import com.google.common.annotations.VisibleForTesting;
import kafka.cluster.Broker;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

/**
 * This monitor detects brokers that don't have brokerstats
 */
public class NoBrokerstatsBrokerMonitor extends KafkaMonitor {
  public KafkaState observe(KafkaContext ctx, KafkaState state) {
    // check if there is any broker that do not have stats.
    List<Broker> noStatsBrokers = getNoBrokerstatsBrokers(ctx);
    if (!noStatsBrokers.isEmpty()) {
      state.setNoBrokerstatsBrokers(noStatsBrokers);
    }
    return state;
  }
  /**
   *   return the list of brokers that do not have stats
   */
  @VisibleForTesting
  protected List<Broker> getNoBrokerstatsBrokers(KafkaContext ctx) {
    Seq<Broker> brokerSeq = ctx.getZkUtils().getAllBrokersInCluster();
    List<Broker> brokers = scala.collection.JavaConverters.seqAsJavaList(brokerSeq);
    List<Broker> noStatsBrokers = new ArrayList<>();

    brokers.stream().forEach(broker -> {
      if (ctx.getKafkaCluster().getBroker(broker.id()) == null) {
        noStatsBrokers.add(broker);
      }
    });
    return noStatsBrokers;
  }
}
