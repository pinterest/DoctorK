package com.pinterest.doctorkafka.modules.monitor.cluster;

import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.context.cluster.ClusterContext;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.state.State;
import com.pinterest.doctorkafka.modules.state.cluster.ClusterState;

public abstract class ClusterMonitor implements Monitor {
  @Override
  public final State observe(Context ctx, State state) throws Exception {
    if (ctx instanceof ClusterContext && state instanceof ClusterState){
      return observe((ClusterContext) ctx, (ClusterState) state);
    }
    return state;
  }

  public abstract ClusterState observe(ClusterContext ctx, ClusterState state) throws Exception;
}
