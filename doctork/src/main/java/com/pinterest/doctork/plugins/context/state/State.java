package com.pinterest.doctork.plugins.context.state;

import com.pinterest.doctork.plugins.context.Context;
import com.pinterest.doctork.plugins.monitor.Monitor;
import com.pinterest.doctork.plugins.operator.Operator;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * State is a set of attributes derived by {@link Monitor Monitors} used to describe the cluster.
 * State is used by {@link Operator Operators} to build operations plans.
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
