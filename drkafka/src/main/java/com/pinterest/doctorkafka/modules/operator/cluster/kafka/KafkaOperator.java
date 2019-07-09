package com.pinterest.doctorkafka.modules.operator.cluster.kafka;

import com.pinterest.doctorkafka.modules.context.cluster.ClusterContext;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.operator.cluster.ClusterOperator;
import com.pinterest.doctorkafka.modules.state.cluster.ClusterState;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

public abstract class KafkaOperator extends ClusterOperator  {

  @Override
  public final boolean operate(ClusterContext ctx, ClusterState state) throws Exception {
    if (ctx instanceof KafkaContext && state instanceof KafkaState) {
      return operate((KafkaContext) ctx, (KafkaState) state);
    }
    return false;
  }

  public abstract boolean operate(KafkaContext ctx, KafkaState state) throws Exception;
}
