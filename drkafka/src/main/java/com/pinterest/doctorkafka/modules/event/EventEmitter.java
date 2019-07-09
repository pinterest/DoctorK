package com.pinterest.doctorkafka.modules.event;

/**
 * Since operators are not registered in the {@link EventListener}, {@link EventEmitter}
 * provides an interface for Operators to emit events
 */

public interface EventEmitter {
  void emit(Event event) throws Exception;
}
