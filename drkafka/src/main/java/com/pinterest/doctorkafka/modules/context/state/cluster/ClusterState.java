package com.pinterest.doctorkafka.modules.context.state.cluster;

import com.pinterest.doctorkafka.modules.context.state.State;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ClusterState extends State {
  private AtomicBoolean underMaintenance = new AtomicBoolean(false);
  private String clusterName;

  public Boolean isUnderMaintenance() {
    return underMaintenance.get();
  }
  public void setUnderMaintenance(boolean maintenance) {
    underMaintenance.set(maintenance);
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }
}
