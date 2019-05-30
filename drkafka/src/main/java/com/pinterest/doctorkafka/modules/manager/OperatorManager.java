package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.operator.Operator;

import org.apache.commons.configuration2.Configuration;

public interface OperatorManager {
  void configureOperator(Operator operator, Configuration additionalConfig) throws Exception;
}
