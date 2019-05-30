package com.pinterest.doctorkafka.modules.monitor.cluster;

import com.pinterest.doctorkafka.modules.context.cluster.ClusterContext;
import com.pinterest.doctorkafka.modules.state.cluster.ClusterState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MaintenanceChecker extends ClusterMonitor {
  private static final Logger LOG = LogManager.getLogger(MaintenanceChecker.class);

  public ClusterState observe(ClusterContext ctx, ClusterState state) {
    if (ctx.isUnderMaintenance()) {
      LOG.debug("Cluster:" + ctx.getClusterName() + " is in maintenance mode");
      state.stop();
    }
    return state;
  }
}
