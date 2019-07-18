package com.pinterest.doctorkafka.modules.operator.cluster;

import com.pinterest.doctorkafka.modules.operator.Operator;
import com.pinterest.doctorkafka.modules.context.state.State;
import com.pinterest.doctorkafka.modules.context.state.cluster.ClusterState;

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
