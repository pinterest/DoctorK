package com.pinterest.doctorkafka.api;

import com.pinterest.doctorkafka.DoctorKafka;
import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.KafkaClusterManager;

import javax.ws.rs.NotFoundException;

public abstract class DoctorKafkaApi {
  private DoctorKafka drkafka;

  public DoctorKafkaApi() {
    this.drkafka = null;
  }

  public DoctorKafkaApi(DoctorKafka drkafka) {
    this.drkafka = drkafka;
  }

  protected DoctorKafka getDrkafka() {
    return drkafka;
  }

  protected KafkaClusterManager checkAndGetClusterManager(String clusterName) {
    KafkaClusterManager clusterManager = drkafka.getClusterManager(clusterName);
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