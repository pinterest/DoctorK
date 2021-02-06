package com.pinterest.doctork.plugins.manager;

import com.pinterest.doctork.plugins.monitor.Monitor;
import com.pinterest.doctork.plugins.operator.Operator;
import com.pinterest.doctork.plugins.task.TaskHandler;

import org.apache.commons.configuration2.AbstractConfiguration;

/**
 * PluginManager is a interface to retrieve plugin ({@link Monitor}, {@link Operator}, {@link TaskHandler})
 * classes based on configurations
 *
 * See {@link DoctorKPluginManager} for the default implementation
 */
public interface PluginManager {

  /**
   * @param monitorConfig The configuration including plugin name, plugin class and plugin-specific settings
   * @return {@link Monitor} instance described in monitorConfig
   * @throws Exception Fails to get the monitor instance
   */
  Monitor getMonitor(AbstractConfiguration monitorConfig) throws Exception;
  /**
   * @param operatorConfig The configuration including plugin name, plugin class and plugin-specific settings
   * @return {@link Operator} instance described in operatorConfig
   * @throws Exception Fails to get the operator instance
   */
  Operator getOperator(AbstractConfiguration operatorConfig) throws Exception;
  /**
   * @param actionConfig The configuration including plugin name, plugin class and plugin-specific settings
   * @return {@link TaskHandler} instance described in actionConfig
   * @throws Exception Fails to get the action instance
   */
  TaskHandler getAction(AbstractConfiguration actionConfig) throws Exception;
}
