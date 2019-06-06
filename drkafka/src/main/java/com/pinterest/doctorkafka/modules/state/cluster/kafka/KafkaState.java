package com.pinterest.doctorkafka.modules.state.cluster.kafka;

import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.modules.state.cluster.ClusterState;

import kafka.cluster.Broker;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

public class KafkaState extends ClusterState {

  private List<PartitionInfo> underReplicatedPartitions;
  private List<Broker> noBrokerstatsBrokers;
  private List<KafkaBroker> toBeReplacedBrokers;

  public List<PartitionInfo> getUnderReplicatedPartitions() {
    return underReplicatedPartitions;
  }

  public void setUnderReplicatedPartitions(
      List<PartitionInfo> underReplicatedPartitions) {
    this.underReplicatedPartitions = underReplicatedPartitions;
  }

  public List<Broker> getNoBrokerstatsBrokers() {
    return noBrokerstatsBrokers;
  }

  public void setNoBrokerstatsBrokers(List<Broker> noBrokerstatsBrokers) {
    this.noBrokerstatsBrokers = noBrokerstatsBrokers;
  }

  public List<KafkaBroker> getToBeReplacedBrokers() {
    return toBeReplacedBrokers;
  }

  public void setToBeReplacedBrokers(List<KafkaBroker> toBeReplacedBrokers) {
    this.toBeReplacedBrokers = toBeReplacedBrokers;
  }
}
