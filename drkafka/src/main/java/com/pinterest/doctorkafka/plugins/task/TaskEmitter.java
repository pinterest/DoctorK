package com.pinterest.doctorkafka.plugins.task;

/**
 * Since operators are not registered in the {@link TaskDispatcher}, {@link TaskEmitter}
 * provides an interface for {@link com.pinterest.doctorkafka.plugins.operator.Operator Operators} to emit tasks
 */

public interface TaskEmitter {
  
  void emit(Task task) throws Exception;
  
}