package com.pinterest.doctorkafka.modules.operator.cluster.kafka;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.event.Event;
import com.pinterest.doctorkafka.modules.event.GenericEvent;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BrokerReplacer extends KafkaOperator {
  private static final Logger LOG = LogManager.getLogger(BrokerReplacer.class);

  private static final String CONFIG_REPLACEMENT_COOLDOWN_SECONDS_KEY = "replacement.cooldown.seconds";

  private long configReplacementCooldownSeconds = 43200L;

  private static final String EVENT_REPLACE_INSTANCE_NAME = "replace_instance";

  private static final String EVENT_CLUSTER_NAME_KEY = "cluster_name";
  private static final String EVENT_HOSTNAME_KEY = "hostname";

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
    configReplacementCooldownSeconds = config.getLong(
        CONFIG_REPLACEMENT_COOLDOWN_SECONDS_KEY,
        configReplacementCooldownSeconds
    );
  }

  @Override
  public boolean operate(KafkaContext ctx, KafkaState state) {
    List<KafkaBroker> toBeReplacedBrokers = state.getToBeReplacedBrokers();
    KafkaBroker victim = null;
    long now = System.currentTimeMillis();
    for (KafkaBroker broker : toBeReplacedBrokers ) {
      if(isClusterReplacementCooldownExpired(ctx, now, broker)){
        victim = broker;
      }
    }

    if (victim != null){
      String brokerName = victim.getName();
      String clusterName = ctx.getClusterName();
      try {
        emit(createReplaceInstanceEvent(clusterName, brokerName));
      } catch (Exception e){
        LOG.error("Failed to emit replacement event", e);
      }
    }

    return true;
  }

  protected Event createReplaceInstanceEvent(String clusterName, String hostname){
    Map<String, Object> replaceInstanceAttributes = new HashMap<>();
    replaceInstanceAttributes.put(EVENT_CLUSTER_NAME_KEY, clusterName);
    replaceInstanceAttributes.put(EVENT_HOSTNAME_KEY, hostname);
    return new GenericEvent(EVENT_REPLACE_INSTANCE_NAME, replaceInstanceAttributes);
  }

  protected boolean isClusterReplacementCooldownExpired(KafkaContext ctx, long now, KafkaBroker victim){
    String clusterName = ctx.getClusterName();
    try {
      long lastReplacementTime =
          ctx.getKafkaClusterZookeeperClient().getLastBrokerReplacementTime(clusterName);
      long elaspedTimeInSeconds = (now - lastReplacementTime) / 1000;
      if (elaspedTimeInSeconds < configReplacementCooldownSeconds) {
        LOG.info("Cannot replace {}:{} due to replace frequency limitation",
            clusterName, victim.getName());
        return false;
      }
    } catch (Exception e) {
      LOG.error("Failed to check last broker replacement info for {}", clusterName, e);
      return false;
    }
    return true;
  }
}
