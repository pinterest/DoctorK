package com.pinterest.doctorkafka.api.dto;

import java.io.Serializable;

public class ClusterSummary implements Serializable {

  private static final long serialVersionUID = 1L;
  private String id;
  private String name;
  private String zk;
  private int alerts;
  private int operations;
  private int numTopics;
  private int numBrokers;

  public ClusterSummary(String id,
                        String name,
                        String zk,
                        int alerts,
                        int operations,
                        int numTopics,
                        int numBrokers) {
    super();
    this.id = id;
    this.name = name;
    this.zk = zk;
    this.alerts = alerts;
    this.operations = operations;
    this.numTopics = numTopics;
    this.numBrokers = numBrokers;
  }

  /**
   * @return the zk
   */
  public String getZk() {
    return zk;
  }

  /**
   * @param zk the zk to set
   */
  public void setZk(String zk) {
    this.zk = zk;
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the alerts
   */
  public int getAlerts() {
    return alerts;
  }

  /**
   * @param alerts the alerts to set
   */
  public void setAlerts(int alerts) {
    this.alerts = alerts;
  }

  /**
   * @return the operations
   */
  public int getOperations() {
    return operations;
  }

  /**
   * @param operations the operations to set
   */
  public void setOperations(int operations) {
    this.operations = operations;
  }

  /**
   * @return the numTopics
   */
  public int getNumTopics() {
    return numTopics;
  }

  /**
   * @param numTopics the numTopics to set
   */
  public void setNumTopics(int numTopics) {
    this.numTopics = numTopics;
  }

  /**
   * @return the numBrokers
   */
  public int getNumBrokers() {
    return numBrokers;
  }

  /**
   * @param numBrokers the numBrokers to set
   */
  public void setNumBrokers(int numBrokers) {
    this.numBrokers = numBrokers;
  }

}
