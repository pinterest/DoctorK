package com.pinterest.doctork.plugins.monitor.cluster.kafka;

import com.pinterest.doctork.plugins.context.state.cluster.kafka.KafkaState;

import com.google.common.annotations.VisibleForTesting;
import kafka.cluster.Broker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

/**
 * This monitor detects brokers that don't have brokerstats
 */
public class NoBrokerstatsBrokerMonitor extends KafkaMonitor {

  private static Logger LOG = LogManager.getLogger(NoBrokerstatsBrokerMonitor.class);

  public KafkaState observe(KafkaState state) {
    // check if there is any broker that do not have stats.
    List<Broker> noStatsBrokers = getNoBrokerstatsBrokers(state);
    if (noStatsBrokers != null && !noStatsBrokers.isEmpty()) {
      state.setNoBrokerstatsBrokers(noStatsBrokers);
    }
    return state;
  }
  /**
   *   return the list of brokers that do not have stats
   */
  @VisibleForTesting
  protected List<Broker> getNoBrokerstatsBrokers(KafkaState state) {
    if(state.getKafkaCluster() == null) {
      return null;
    }
    Seq<Broker> brokerSeq = state.getZkUtils().getAllBrokersInCluster();
    List<Broker> brokers = scala.collection.JavaConverters.seqAsJavaList(brokerSeq);
    List<Broker> noStatsBrokers = new ArrayList<>();

    brokers.stream().forEach(broker -> {
      if (state.getKafkaCluster().getBroker(broker.id()) == null) {
        noStatsBrokers.add(broker);
      }
    });
    return noStatsBrokers;
  }
}
