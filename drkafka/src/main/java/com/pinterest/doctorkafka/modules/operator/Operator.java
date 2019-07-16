package com.pinterest.doctorkafka.modules.operator;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.context.event.Event;
import com.pinterest.doctorkafka.modules.context.event.EventEmitter;
import com.pinterest.doctorkafka.modules.context.state.State;


/**
 * An operator takes the state of a service and performs some operations to remediate/alert on the service.
 * Operators emit {@link Event Event(s)} to trigger these actions.
 */
public abstract class Operator implements Configurable {
  private EventEmitter eventEmitter;

  public void setEventEmitter(EventEmitter eventEmitter){
    this.eventEmitter = eventEmitter;
  }

  /**
   * Sends an event to trigger actions
   * @param event Event that provides context to the subscribed action(s)
   * @throws Exception
   */
  public void emit(Event event) throws Exception {
    eventEmitter.emit(event);
  }

  /**
   * @param state The state derived from {@link com.pinterest.doctorkafka.modules.monitor.Monitor Monitors}
   * @return false if later operations should not be executed, true otherwise.
   * @throws Exception
   */
  public abstract boolean operate(State state) throws Exception;
}
