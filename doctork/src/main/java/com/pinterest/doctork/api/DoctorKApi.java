package com.pinterest.doctork.api;

import com.pinterest.doctork.DoctorK;
import com.pinterest.doctork.KafkaBroker;
import com.pinterest.doctork.KafkaClusterManager;

import javax.ws.rs.NotFoundException;

public abstract class DoctorKApi {
  private DoctorK doctorK;

  public DoctorKApi() {
    this.doctorK = null;
  }

  public DoctorKApi(DoctorK doctorK) {
    this.doctorK = doctorK;
  }

  protected DoctorK getDoctorK() {
    return doctorK;
  }

  protected KafkaClusterManager checkAndGetClusterManager(String clusterName) {
    KafkaClusterManager clusterManager = doctorK.getClusterManager(clusterName);
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