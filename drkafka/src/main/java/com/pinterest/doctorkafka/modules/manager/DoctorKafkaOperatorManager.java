package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.operator.Operator;

import org.apache.commons.configuration2.Configuration;

public class DoctorKafkaOperatorManager extends ModuleManager implements OperatorManager {
  public DoctorKafkaOperatorManager(Configuration config){
    super(config);
  }

  @Override
  public void configureOperator(Operator operator, Configuration additionalConfig) throws Exception {
    Configuration config = getDefaultConfig(operator.getConfigName());
    operator.configure(additionalConfig, config);
  }
}
