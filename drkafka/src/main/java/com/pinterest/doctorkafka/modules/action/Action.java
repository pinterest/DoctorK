package com.pinterest.doctorkafka.modules.action;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.context.event.Event;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

/**
 * Actions are modules that perform simple operations based on events. They should be subscribed to
 * the event handler from the cluster manager.
 */
public abstract class Action implements Configurable {
  private static final Logger LOG = LogManager.getLogger(Action.class);
  private static final String CONFIG_SUBSCRIBED_EVENTS_KEY = "subscribed_events";
  private static final String CONFIG_DRY_RUN_KEY = "dryrun";

  private String[] subscribedEvents;
  private boolean dryrun = true;

  /**
   * This method is called when an event that this Action has subscribed to has been fired.
   * @param event Event that the action has subscribed to
   * @return A set of Events that will be fired after this action has been executed.
   * @throws Exception
   */
  public final Collection<Event> on(Event event) throws Exception{
    if(dryrun) {
      return null;
    } else {
      LOG.info("Dry run: Action {} triggered by event {}", this.getClass(), event.getName());
      return execute(event);
    }
  }

  /**
   * This method is called to execute the main logic of an Action
   * @param event
   * @return A set of Events that will be fired after this action has been executed.
   * @throws Exception
   */
  public abstract Collection<Event> execute(Event event) throws Exception;

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    if (!config.containsKey(CONFIG_SUBSCRIBED_EVENTS_KEY)){
      throw new ModuleConfigurationException("Missing event subscriptions for action " + this.getClass());
    }
    subscribedEvents = config.getString(CONFIG_SUBSCRIBED_EVENTS_KEY).split(",");
    dryrun = config.getBoolean(CONFIG_DRY_RUN_KEY, dryrun);
  }

  public final String[] getSubscribedEvents(){
    return subscribedEvents;
  }
}
