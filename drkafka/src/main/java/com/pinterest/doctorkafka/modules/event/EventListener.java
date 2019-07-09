package com.pinterest.doctorkafka.modules.event;

import com.pinterest.doctorkafka.modules.action.Action;

/**
 * EventListener is a instance that receives {@link Event}s from modules (e.g. Operators, Actions)
 * and calls actions that have subscribed to the event.
 */
public interface EventListener {

  /**
   * Receives an Event generated externally and invokes corresponding actions
   * @param event
   * @throws Exception
   */
  void receive(Event event) throws Exception;

  /**
   * subscribes the action to an event
   * @param eventName the name of the event the action is subscribing to
   * @param action
   * @throws Exception
   */
  void subscribe(String eventName, Action action) throws Exception;
}
