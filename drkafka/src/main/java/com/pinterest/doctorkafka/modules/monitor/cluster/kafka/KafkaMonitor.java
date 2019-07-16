package com.pinterest.doctorkafka.modules.monitor.cluster.kafka;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.monitor.cluster.ClusterMonitor;
import com.pinterest.doctorkafka.modules.context.state.cluster.ClusterState;
import com.pinterest.doctorkafka.modules.context.state.cluster.kafka.KafkaState;

import org.apache.commons.configuration2.AbstractConfiguration;

public abstract class KafkaMonitor extends ClusterMonitor {
  @Override
  public final ClusterState observe(ClusterState state) throws Exception{
    if (state instanceof KafkaState){
      return observe((KafkaState) state);
    }
    return null;
  }
  public abstract KafkaState observe(KafkaState state) throws Exception;

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
  }
}
