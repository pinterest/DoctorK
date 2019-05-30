package com.pinterest.doctorkafka.modules.operator.cluster.kafka;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.modules.action.ReportOperation;
import com.pinterest.doctorkafka.modules.action.SendEvent;
import com.pinterest.doctorkafka.modules.action.cluster.ReplaceInstance;
import com.pinterest.doctorkafka.modules.action.errors.ProlongReplacementException;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BrokerReplacer extends KafkaOperator {
  private static final Logger LOG = LogManager.getLogger(BrokerReplacer.class);

  private static final String CONFIG_NAME = "broker_replacer";

  private static final String CONFIG_REPLACEMENT_COOLDOWN_SECONDS_KEY = "replacement.cooldown.seconds";
  private static final String CONFIG_PROLONG_REPLACEMENT_ALERT_SNOOZE_SECONDS_KEY = "prolong.replacement.alert.snooze.seconds";

  private static final long DEFAULT_REPLACEMENT_COOLDOWN_SECONDS = 43200L;
  private static final long DEFAULT_PROLONG_REPLACEMENT_ALERT_SNOOZE_SECONDS = 1200L;

  private long configReplacementCooldownSeconds;
  private long configProlongReplacementAlertSnoozeTime;

  private long prevProlongReplacementAlertTime = 0L;

  private ReplaceInstance replaceInstance;
  private SendEvent sendEvent;
  private ReportOperation reportOperation;

  public BrokerReplacer(SendEvent sendEvent, ReplaceInstance replaceInstance, ReportOperation reportOperation) {
    this.replaceInstance = replaceInstance;
    this.sendEvent = sendEvent;
    this.reportOperation = reportOperation;
  }

  @Override
  public String getConfigName() {
    return CONFIG_NAME;
  }

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
    configReplacementCooldownSeconds = config.getLong(
        CONFIG_REPLACEMENT_COOLDOWN_SECONDS_KEY,
        DEFAULT_REPLACEMENT_COOLDOWN_SECONDS
    );
    configProlongReplacementAlertSnoozeTime = config.getLong(
        CONFIG_PROLONG_REPLACEMENT_ALERT_SNOOZE_SECONDS_KEY,
        DEFAULT_PROLONG_REPLACEMENT_ALERT_SNOOZE_SECONDS
    );
  }

  @Override
  public boolean operate(KafkaContext ctx, KafkaState state) throws Exception {
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
      if (super.isDryRun()){
        LOG.info("Dry run: Replacing {} in {}", brokerName, clusterName);
      } else {
        try {
          replaceInstance.replace(brokerName);
          sendEvent.notify(clusterName + "replacing broker " + brokerName,
              "Replacing broker " + brokerName + " on cluster " + clusterName);
          reportOperation.report(clusterName, "Broker replacement: "+brokerName);
        } catch (ProlongReplacementException e) {
          String title = "Slow replacement of broker " + e.getHostname() + " in cluster " + ctx
              .getClusterName();
          LOG.error(title,e);
          if ((prevProlongReplacementAlertTime - now)/1000 > configProlongReplacementAlertSnoozeTime) {
            sendEvent.alert(title, e.getMessage());
            prevProlongReplacementAlertTime = now;
          }
        }
      }
    }

    return true;
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
