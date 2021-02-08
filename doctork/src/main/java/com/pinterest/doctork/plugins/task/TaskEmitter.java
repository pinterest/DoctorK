package com.pinterest.doctork.plugins.task;

import com.pinterest.doctork.plugins.operator.Operator;

/**
 * Since operators are not registered in the {@link TaskDispatcher}, {@link TaskEmitter}
 * provides an interface for {@link Operator Operators} to emit tasks
 */

public interface TaskEmitter {
  
  void emit(Task task) throws Exception;
  
}