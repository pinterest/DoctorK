package com.pinterest.doctorkafka.modules.state;

import com.pinterest.doctorkafka.modules.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * State is a set of attributes derived by {@link com.pinterest.doctorkafka.modules.monitor.Monitor Monitors} used to describe the cluster.
 * State is used by {@link com.pinterest.doctorkafka.modules.operator.Operator Operators} to generate operations.
 */
public abstract class State extends Context {
  private AtomicBoolean operationsStopped = new AtomicBoolean(false);

  public boolean isOperationsStopped() {
    return operationsStopped.get();
  }
  public void stopOperations() {
    operationsStopped.set(true);
  }
}
