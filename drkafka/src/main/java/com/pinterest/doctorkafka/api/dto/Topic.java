package com.pinterest.doctorkafka.api.dto;

import java.io.Serializable;

public class Topic implements Serializable {

  private static final long serialVersionUID = 1L;

  private String name;
  private String clusterName;
  private int numPartitions;
  private int replicationFactor;
  private int numUrps;
  private double mbsIn;
  private double mbsOut;

  public Topic(String name,
               String clusterName,
               int numPartitions,
               int replicationFactor,
               int numUrps,
               double mbsIn,
               double mbsOut) {
    this.name = name;
    this.clusterName = clusterName;
    this.numPartitions = numPartitions;
    this.replicationFactor = replicationFactor;
    this.numUrps = numUrps;
    this.mbsIn = mbsIn;
    this.mbsOut = mbsOut;
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
   * @return the numPartitions
   */
  public int getNumPartitions() {
    return numPartitions;
  }

  /**
   * @param numPartitions the numPartitions to set
   */
  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  /**
   * @return the replicationFactor
   */
  public int getReplicationFactor() {
    return replicationFactor;
  }

  /**
   * @param replicationFactor the replicationFactor to set
   */
  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  /**
   * @return the numUrps
   */
  public int getNumUrps() {
    return numUrps;
  }

  /**
   * @param numUrps the numUrps to set
   */
  public void setNumUrps(int numUrps) {
    this.numUrps = numUrps;
  }

  /**
   * @return the clusterName
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * @param clusterName the clusterName to set
   */
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  /**
   * @return the mbsIn
   */
  public double getMbsIn() {
    return mbsIn;
  }

  /**
   * @param mbsIn the mbsIn to set
   */
  public void setMbsIn(double mbsIn) {
    this.mbsIn = mbsIn;
  }

  /**
   * @return the mbsOut
   */
  public double getMbsOut() {
    return mbsOut;
  }

  /**
   * @param mbsOut the mbsOut to set
   */
  public void setMbsOut(double mbsOut) {
    this.mbsOut = mbsOut;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Topic) {
      Topic t = ((Topic) obj);
      return t.name.equals(name);
    } else {
      return false;
    }
  }

}
