package com.pinterest.doctorkafka.plugins.operator;

import com.pinterest.doctorkafka.plugins.Configurable;
import com.pinterest.doctorkafka.plugins.context.event.Event;
import com.pinterest.doctorkafka.plugins.context.event.EventEmitter;
import com.pinterest.doctorkafka.plugins.context.state.State;


/**
 * An operator takes the {@link State} of a service and builds a plan of operations to remediate/alert on the service.
 * Operators emit {@link Event Events} to trigger these actions.
 * <pre>
 +------------+               +-----------+    +-----------+
 |            |    +-----+    |           |    |           |
 |  Monitor   |+-->|State|+-->| Operator1 |+-->| Operator2 |
 |            |    +-----+    |           |    |           |
 +------------+               +-----------+    +-----------+
                                    +                +
                                    | emit           | emit
                                    v                v
                                 +-----+          +-----+
                                 |Event|          |Event|
                                 +-----+          +-----+
                                    +                +
                                    |                |
                                    +----------------+
                                            |
                                            v
                                    Actions/Event Handling
 </pre>
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
   * @param state The state derived from {@link com.pinterest.doctorkafka.plugins.monitor.Monitor Monitors}
   * @return false if later operations should not be executed, true otherwise.
   * @throws Exception
   */
  public abstract boolean operate(State state) throws Exception;
}
