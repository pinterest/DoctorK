package com.pinterest.doctork.plugins.monitor.cluster;

import com.pinterest.doctork.plugins.context.state.State;
import com.pinterest.doctork.plugins.context.state.cluster.ClusterState;
import com.pinterest.doctork.plugins.monitor.Monitor;

public abstract class ClusterMonitor extends Monitor {
  @Override
  public final State observe(State state) throws Exception {
    if (state instanceof ClusterState){
      return observe((ClusterState) state);
    }
    return state;
  }

  public abstract ClusterState observe(ClusterState state) throws Exception;
}
