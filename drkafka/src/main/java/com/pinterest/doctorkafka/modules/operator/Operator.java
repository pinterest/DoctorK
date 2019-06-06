package com.pinterest.doctorkafka.modules.operator;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.event.Event;
import com.pinterest.doctorkafka.modules.event.EventEmitter;
import com.pinterest.doctorkafka.modules.state.State;

public abstract class Operator implements Configurable {
  private EventEmitter eventEmitter;

  public void setEventEmitter(EventEmitter eventEmitter){
    this.eventEmitter = eventEmitter;
  }

  public void emit(Event event) throws Exception {
    eventEmitter.emit(event);
  }

  // returns false if later operations should not be executed
  public abstract boolean operate(Context ctx, State state) throws Exception;
}
