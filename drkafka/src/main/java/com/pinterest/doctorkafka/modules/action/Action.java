package com.pinterest.doctorkafka.modules.action;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.event.Event;

import org.apache.commons.configuration2.AbstractConfiguration;

import java.util.Collection;

public abstract class Action implements Configurable {
  private static final String CONFIG_SUBSCRIBED_EVENTS_KEY = "subscribed.events";
  private static final String CONFIG_DRY_RUN_KEY = "dryrun";

  private String[] subscribedEvents;
  private boolean dryrun = true;

  public abstract Collection<Event> execute(Event event) throws Exception;

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    if (!config.containsKey(CONFIG_SUBSCRIBED_EVENTS_KEY)){
      throw new ModuleConfigurationException("Missing event subscriptions for action " + this.getClass());
    }
    subscribedEvents = config.getStringArray(CONFIG_SUBSCRIBED_EVENTS_KEY);
    dryrun = config.getBoolean(CONFIG_DRY_RUN_KEY, dryrun);
  }

  public final String[] getSubscribedEvents(){
    return subscribedEvents;
  }

  public void setDryRun(boolean dryrun) {
    this.dryrun = dryrun;
  }

  public boolean isDryRun() {
    return dryrun;
  }
}
