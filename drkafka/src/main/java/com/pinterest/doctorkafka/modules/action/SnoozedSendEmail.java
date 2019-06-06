package com.pinterest.doctorkafka.modules.action;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.event.Event;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SnoozedSendEmail extends SendEmail {
  private static final Logger LOG = LogManager.getLogger(SnoozedSendEmail.class);
  private static final String CONFIG_SNOOZE_SECONDS_KEY = "snooze.seconds";

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
