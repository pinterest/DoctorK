package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;

/**
 * ModuleManager is a interface to retrieve module ({@link Monitor}, {@link Operator}, {@link Action})
 * classes based on configurations
 *
 * See {@link DoctorKafkaModuleManager} for the default implementation
 */
public interface ModuleManager {
  Monitor getMonitor(AbstractConfiguration monitorConfig) throws Exception;
  Operator getOperator(AbstractConfiguration operatorConfig) throws Exception;
  Action getAction(AbstractConfiguration actionConfig) throws Exception;
}
