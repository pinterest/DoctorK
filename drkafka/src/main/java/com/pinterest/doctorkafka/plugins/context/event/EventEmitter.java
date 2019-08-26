package com.pinterest.doctorkafka.plugins.context.event;

/**
 * Since operators are not registered in the {@link EventListener}, {@link EventEmitter}
 * provides an interface for {@link com.pinterest.doctorkafka.plugins.operator.Operator Operators} to emit events
 */

public interface EventEmitter {
  void emit(Event event) throws Exception;
}
