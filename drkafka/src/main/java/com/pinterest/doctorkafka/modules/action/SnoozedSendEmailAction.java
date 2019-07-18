package com.pinterest.doctorkafka.modules.action;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.context.event.Event;

import org.apache.commons.configuration2.AbstractConfiguration;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This action sends emails to a list of email addresses, but with a cooldown time for each type of event
 *
 * config:
 * [required]
 *   emails: <comma separated list of email addresses to send >
 * [optional]
 *   snooze_seconds: <snooze time for each event. Default: 20 minutes >
 *
 * Input Event Format:
 * {
 *    title: str,
 *    message: str
 * }
 */
public class SnoozedSendEmailAction extends SendEmailAction {
  private static final String CONFIG_SNOOZE_SECONDS_KEY = "snooze_seconds";

  private long configSnoozeSeconds = 1200L;

  private Map<String, Long> prevSentTimePerEvent = new ConcurrentHashMap<>();

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
    configSnoozeSeconds = config.getLong(CONFIG_SNOOZE_SECONDS_KEY, configSnoozeSeconds);
  }

  @Override
  public Collection<Event> execute(Event event) throws Exception {
    String eventName = event.getName();
    if (hasSnoozeExpired(eventName)){
      prevSentTimePerEvent.put(eventName, System.currentTimeMillis());
      return execute(event);
    }
    return null;
  }

  protected boolean hasSnoozeExpired(String eventName) {
    long prevSentTime = prevSentTimePerEvent.computeIfAbsent(eventName, k -> -1L);
    return ( System.currentTimeMillis() - prevSentTime ) > configSnoozeSeconds * 1000L;
  }
}
