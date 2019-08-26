package com.pinterest.doctorkafka.plugins.monitor.cluster.kafka;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.context.state.cluster.kafka.KafkaState;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This monitor detects dead brokers based on the BrokerStats and KafkaCluster provided by BrokerStatsMonitor.
 *
 * <pre>
 * config:
 * [optional]
 *   no_stats_seconds: <number of seconds not receiving brokerstats before marking a broker dead>
 * </pre>
 */
public class DeadBrokerMonitor extends KafkaMonitor {
  private static final Logger LOG = LogManager.getLogger(DeadBrokerMonitor.class);

  private static final String CONFIG_NO_STATS_SECONDS_KEY = "no_stats_seconds";

  private long configNoStatsSeconds = 1200L;

  @Override
  public void configure(AbstractConfiguration config) throws PluginConfigurationException {
    super.configure(config);
    configNoStatsSeconds = config.getLong(CONFIG_NO_STATS_SECONDS_KEY, configNoStatsSeconds);
  }

  public KafkaState observe(KafkaState state) {
    long now = System.currentTimeMillis();
    state.setToBeReplacedBrokers(getBrokersToReplace(state, now));
    return state;
  }

  protected List<KafkaBroker> getBrokersToReplace(KafkaState state, long now) {
    List<KafkaBroker> toBeReplacedBrokers = new ArrayList<>();
    for (Map.Entry<Integer, KafkaBroker> brokerEntry : state.getKafkaCluster().brokers.entrySet()) {
      KafkaBroker broker = brokerEntry.getValue();
      double lastUpdateTime = (now - broker.getLastStatsTimestamp()) / 1000.0;
      // call broker replacement script to replace dead brokers
      if (lastUpdateTime > configNoStatsSeconds) {
        toBeReplacedBrokers.add(broker);
      }
    }
    return toBeReplacedBrokers;
  }
}
