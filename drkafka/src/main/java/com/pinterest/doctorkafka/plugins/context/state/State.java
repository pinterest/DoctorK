package com.pinterest.doctorkafka.plugins.context.state;

import com.pinterest.doctorkafka.plugins.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * State is a set of attributes derived by {@link com.pinterest.doctorkafka.plugins.monitor.Monitor Monitors} used to describe the cluster.
 * State is used by {@link com.pinterest.doctorkafka.plugins.operator.Operator Operators} to build operations plans.
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
