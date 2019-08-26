package com.pinterest.doctorkafka.plugins.operator.cluster.kafka;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.context.event.Event;
import com.pinterest.doctorkafka.plugins.context.event.EventUtils;
import com.pinterest.doctorkafka.plugins.context.event.GenericEvent;
import com.pinterest.doctorkafka.plugins.context.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.util.ZookeeperClient;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This operator selects a offline broker, checks the record on zookeeper to see if the cooldown period has passed,
 * and emits an event to replace the broker if appropriate.
 *
 * <pre>
 * config:
 *   replacement_cooldown_seconds: <number of seconds before next replacement is allowed (Default: 43200)>
 *
 * Output Events Format:
 * Event: replace_instance:
 * triggered to replace an instance
 * {
 *   cluster_name: str,
 *   hostname: str,
 *   zookeeper_client: com.pinterest.doctorkafka.util.ZookeeperClient
 * }
 * </pre>
 */
public class BrokerReplacementOperator extends KafkaOperator {
  private static final Logger LOG = LogManager.getLogger(BrokerReplacementOperator.class);

  private static final String CONFIG_REPLACEMENT_COOLDOWN_SECONDS_KEY = "replacement_cooldown_seconds";

  private long configReplacementCooldownSeconds = 43200L;

  private static final String EVENT_REPLACE_INSTANCE_NAME = "replace_instance";

  private static final String EVENT_HOSTNAME_KEY = "hostname";
  private static final String EVENT_ZOOKEEPER_CLIENT_KEY = "zookeeper_client";

  @Override
  public void configure(AbstractConfiguration config) throws PluginConfigurationException {
    super.configure(config);
    configReplacementCooldownSeconds = config.getLong(
        CONFIG_REPLACEMENT_COOLDOWN_SECONDS_KEY,
        configReplacementCooldownSeconds
    );
  }

  @Override
  public boolean operate(KafkaState state) {
    List<KafkaBroker> toBeReplacedBrokers = state.getToBeReplacedBrokers();
    KafkaBroker victim = null;
    if ( toBeReplacedBrokers != null && toBeReplacedBrokers.size() > 0 ){
      if(!isClusterReplacementCooldownExpired(state)) {
        LOG.info("Cannot replace brokers on {} due to replace frequency limitation", state.getClusterName());
      } else {
        victim = toBeReplacedBrokers.get(0);
      }
    }

    if (victim != null){
      String brokerName = victim.getName();
      String clusterName = state.getClusterName();
      try {
        emit(createReplaceInstanceEvent(clusterName, brokerName, state.getKafkaClusterZookeeperClient()));
      } catch (Exception e){
        LOG.error("Failed to emit replacement event", e);
      }
    }

    return true;
  }

  protected Event createReplaceInstanceEvent(String clusterName, String hostname, ZookeeperClient zookeeperClient){
    Map<String, Object> replaceInstanceAttributes = new HashMap<>();
    replaceInstanceAttributes.put(EventUtils.EVENT_CLUSTER_NAME_KEY, clusterName);
    replaceInstanceAttributes.put(EVENT_HOSTNAME_KEY, hostname);
    replaceInstanceAttributes.put(EVENT_ZOOKEEPER_CLIENT_KEY, zookeeperClient);
    return new GenericEvent(EVENT_REPLACE_INSTANCE_NAME, replaceInstanceAttributes);
  }

  protected boolean isClusterReplacementCooldownExpired(KafkaState state){
    String clusterName = state.getClusterName();
    try {
      long lastReplacementTime =
          state.getKafkaClusterZookeeperClient().getLastBrokerReplacementTime(clusterName);
      long elapsedTimeInSeconds = (System.currentTimeMillis()- lastReplacementTime) / 1000;
      return elapsedTimeInSeconds > configReplacementCooldownSeconds;
    } catch (Exception e) {
      LOG.error("Failed to check last broker replacement info for {}", clusterName, e);
      return false;
    }
  }
}
