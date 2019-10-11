package com.pinterest.doctorkafka.plugins.context.event;

import com.pinterest.doctorkafka.plugins.action.Action;

/**
 * EventDispatcher is a instance that dispatches {@link Event}s that were received from plugins (e.g. {@link com.pinterest.doctorkafka.plugins.operator.Operator Operators}
 * , {@link Action Actions})
 * to actions that have subscribed to the event.
 */
public interface EventDispatcher {

  /**
   * dispatches an Event to corresponding actions
   * @param event
   * @throws Exception
   */
  void dispatch(Event event) throws Exception;

  /**
   * subscribes the action to an event
   * @param eventName the name of the event the action is subscribing to
   * @param action
   * @throws Exception
   */
  void subscribe(String eventName, Action action) throws Exception;

  /**
   * start dispatching events
   */

  void start();

  /**
   * stop dispatching events
   */

  void stop();

}
