package com.pinterest.doctorkafka.modules.context.cluster;

import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.context.Maintainable;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterContext extends Context implements Maintainable {
  private AtomicBoolean underMaintenance = new AtomicBoolean(false);
  private String clusterName;

  public Boolean isUnderMaintenance() {
    return underMaintenance == null ? null : underMaintenance.get();
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
