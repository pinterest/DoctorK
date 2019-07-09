package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;

import org.apache.commons.configuration2.Configuration;

/**
 * ModuleManager is a interface to retrieve module ({@link Monitor}, {@link Operator}, {@link Action})
 * classes based on configurations
 *
 * See {@link DoctorKafkaModuleManager} for the default implementation
 */
public interface ModuleManager {
  Monitor getMonitor(String name, Configuration additionalConfig) throws Exception;
  Operator getOperator(String name, Configuration additionalConfig) throws Exception;
  Action getAction(String name, Configuration additionalConfig) throws Exception;
}
