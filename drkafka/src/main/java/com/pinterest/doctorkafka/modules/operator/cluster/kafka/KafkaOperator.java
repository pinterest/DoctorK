package com.pinterest.doctorkafka.modules.operator.cluster.kafka;

import com.pinterest.doctorkafka.modules.operator.cluster.ClusterOperator;
import com.pinterest.doctorkafka.modules.context.state.cluster.ClusterState;
import com.pinterest.doctorkafka.modules.context.state.cluster.kafka.KafkaState;

public abstract class KafkaOperator extends ClusterOperator  {

  @Override
  public final boolean operate(ClusterState state) throws Exception {
    if (state instanceof KafkaState) {
      return operate((KafkaState) state);
    }
    return false;
  }

  public abstract boolean operate(KafkaState state) throws Exception;
}
