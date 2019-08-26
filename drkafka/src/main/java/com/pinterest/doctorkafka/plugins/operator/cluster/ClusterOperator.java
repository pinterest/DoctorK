package com.pinterest.doctorkafka.plugins.operator.cluster;

import com.pinterest.doctorkafka.plugins.operator.Operator;
import com.pinterest.doctorkafka.plugins.context.state.State;
import com.pinterest.doctorkafka.plugins.context.state.cluster.ClusterState;

public abstract class ClusterOperator extends Operator  {
  @Override
  public final boolean operate(State state) throws Exception {
    if (state instanceof ClusterState) {
      return operate((ClusterState) state);
    }
    return false;
  }

  public abstract boolean operate(ClusterState state) throws Exception;
}
