package com.pinterest.doctorkafka.modules.monitor.cluster.kafka;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This monitor detects dead brokers
 *
 * Configuration:
 * [optional]
 * config.no_stats.seconds=<number of seconds not receiving brokerstats before marking a broker dead>
 */
public class DeadBrokerMonitor extends KafkaMonitor {
  private static final Logger LOG = LogManager.getLogger(DeadBrokerMonitor.class);

  private static final String CONFIG_NO_STATS_SECONDS_KEY = "no_stats.seconds";

  private long configNoStatsSeconds = 1200L;

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
    configNoStatsSeconds = config.getLong(CONFIG_NO_STATS_SECONDS_KEY, configNoStatsSeconds);
  }

  public KafkaState observe(KafkaContext ctx, KafkaState state) {
    long now = System.currentTimeMillis();
    state.setToBeReplacedBrokers(getBrokersToReplace(ctx, now));
    return state;
  }

  protected List<KafkaBroker> getBrokersToReplace(KafkaContext ctx, long now) {
    List<KafkaBroker> toBeReplacedBrokers = new ArrayList<>();
    for (Map.Entry<Integer, KafkaBroker> brokerEntry : ctx.getKafkaCluster().brokers.entrySet()) {
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
