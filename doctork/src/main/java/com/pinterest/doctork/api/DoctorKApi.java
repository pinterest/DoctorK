package com.pinterest.doctork.api;

import com.pinterest.doctork.KafkaBroker;
import com.pinterest.doctork.KafkaClusterManager;

import javax.ws.rs.NotFoundException;

public abstract class DoctorKApi {
  private com.pinterest.doctork.DoctorK doctork;

  public DoctorKApi() {
    this.doctork = null;
  }

  public DoctorKApi(com.pinterest.doctork.DoctorK doctork) {
    this.doctork = doctork;
  }

  protected com.pinterest.doctork.DoctorK getDoctorK() {
    return doctork;
  }

  protected KafkaClusterManager checkAndGetClusterManager(String clusterName) {
    KafkaClusterManager clusterManager = doctork.getClusterManager(clusterName);
    if (clusterManager == null) {
      throw new NotFoundException("Unknown clustername:" + clusterName);
    }
    return clusterManager;
  }

  protected KafkaBroker checkAndGetBroker(String clusterName, String brokerId) {
    KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
    Integer id = Integer.parseInt(brokerId);
    KafkaBroker broker = clusterManager.getCluster().getBroker(id);
    if (broker == null) {
      throw new NotFoundException("Unknown brokerId: " + brokerId);
    }
    return broker;
  }

}