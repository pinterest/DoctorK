package com.pinterest.doctorkafka.modules.monitor.cluster.kafka;

import com.pinterest.doctorkafka.modules.context.cluster.ClusterContext;
import com.pinterest.doctorkafka.modules.context.cluster.kafka.KafkaContext;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.monitor.cluster.ClusterMonitor;
import com.pinterest.doctorkafka.modules.state.cluster.ClusterState;
import com.pinterest.doctorkafka.modules.state.cluster.kafka.KafkaState;

import org.apache.commons.configuration2.AbstractConfiguration;

public abstract class KafkaMonitor extends ClusterMonitor {
  @Override
  public final ClusterState observe(ClusterContext ctx, ClusterState state){
    if (ctx instanceof KafkaContext && state instanceof KafkaState){
      return observe((KafkaContext) ctx, (KafkaState) state);
    }
    return null;
  }
  public abstract KafkaState observe(KafkaContext ctx, KafkaState state);

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
  }
}
