package com.pinterest.doctorkafka.modules.operator.cluster;

import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.context.cluster.ClusterContext;
import com.pinterest.doctorkafka.modules.operator.Operator;
import com.pinterest.doctorkafka.modules.state.State;
import com.pinterest.doctorkafka.modules.state.cluster.ClusterState;

public abstract class ClusterOperator extends Operator  {
  @Override
  public final boolean operate(Context ctx, State state) throws Exception {
    if (ctx instanceof ClusterContext && state instanceof ClusterState) {
      return operate((ClusterContext) ctx, (ClusterState) state);
    }
    return false;
  }

  public abstract boolean operate(ClusterContext ctx, ClusterState state) throws Exception;
}
