package com.pinterest.doctorkafka.plugins.action;

import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.plugins.Plugin;
import com.pinterest.doctorkafka.plugins.context.event.Event;
import com.pinterest.doctorkafka.plugins.context.event.EventListener;
import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.operator.Operator;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

/**
 *
 * Actions are plugins that perform single operations based on certain {@link Event}s. On initialization,
 * each action will subscribe to the cluster's {@link EventListener} to the events listed in the configuration.
 *
 * <pre>
 *                                +------------------+
 *                     Subscribe  |                  |
 *            Action +----------->|   EventListener  |
 *                                |                  |
 *                                +------------------+
 * </pre>
 *
 * Once the evaluation loops in {@link KafkaClusterManager} starts and {@link Operator}s emit events,
 * the EventListener will collect the events and dispatch events to Actions that have
 * subscribed to the corresponding event.
 *
 * <pre>
 *               +-----------------+
 *               |                 |
 *               | Operator/Action |
 *               |                 |
 *               +-----------------+
 *                        +
 *                        |              execute +---------+
 *                      Event           +------->| Action  |
 *                        |             |        +---------+
 *                        v             |
 *               +-----------------+    |
 *               |                 |    |execute +---------+
 *               |  EventListener  |+-->+------->| Action  |
 *               |                 |    |        +---------+
 *               +-----------------+    |
 *                                      |
 *                                      |execute +---------+
 *                                      +------->| Action  |
 *                                               +---------+
 *
 * </pre>
 */
public abstract class Action implements Plugin {
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
      LOG.info("Dry run: Action {} triggered by event {}", this.getClass(), event.getName());
      return null;
    } else {
      LOG.debug("Executing Action {} triggered by event {}", this.getClass(), event.getName());
      return execute(event);
    }
  }

  /**
   * This method is called to execute the main logic of an Action
   * @param event Event that the action has subscribed to
   * @return A set of Events that will be fired after this action has been executed.
   * @throws Exception
   */
  public abstract Collection<Event> execute(Event event) throws Exception;

  @Override
  public final void initialize(ImmutableConfiguration config) throws PluginConfigurationException {
    if (!config.containsKey(CONFIG_SUBSCRIBED_EVENTS_KEY)){
      throw new PluginConfigurationException("Missing event subscriptions for action " + this.getClass());
    }
    subscribedEvents = config.getString(CONFIG_SUBSCRIBED_EVENTS_KEY).split(",");
    dryrun = config.getBoolean(CONFIG_DRY_RUN_KEY, dryrun);
    configure(config);
  }

  public final String[] getSubscribedEvents(){
    return subscribedEvents;
  }
}
