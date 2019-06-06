package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;

import org.apache.commons.configuration2.Configuration;

public interface ModuleManager {
  Monitor getMonitor(String name, Configuration additionalConfig) throws Exception;
  Operator getOperator(String name, Configuration additionalConfig) throws Exception;
  Action getAction(String name, Configuration additionalConfig) throws Exception;
}
