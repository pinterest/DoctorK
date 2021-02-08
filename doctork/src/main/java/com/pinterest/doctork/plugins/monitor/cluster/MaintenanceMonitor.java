package com.pinterest.doctork.plugins.monitor.cluster;

import com.pinterest.doctork.plugins.context.state.cluster.ClusterState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This monitor checks if the cluster is set in maintenance mode
 */
public class MaintenanceMonitor extends ClusterMonitor {
  private static final Logger LOG = LogManager.getLogger(MaintenanceMonitor.class);

  public ClusterState observe(ClusterState state) {
    if (state.isUnderMaintenance()) {
      LOG.info("Cluster:" + state.getClusterName() + " is in maintenance mode");
      state.stopOperations();
    }
    return state;
  }
}
