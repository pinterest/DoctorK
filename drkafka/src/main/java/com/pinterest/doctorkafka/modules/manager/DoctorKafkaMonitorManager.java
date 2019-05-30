package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.monitor.Monitor;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * DoctorKafkaMonitorManager is a simple implementation of MonitorManager.
 * It will search for the plugin in the classpath and apply configuration to the plugins.
 * Additional configurations can be applied and will have a higher priority compared to plugin
 * configurations.
 *
 * Plugin config should be structured as the following:
 * plugin-1.class=com.foo.bar.plugin1
 * plugin-1.config.some.attribute=foo
 * plugin-2.class=com.foo.bar.plugin2
 * plugin-2.config.some.other.attribute=bar
 */

public class DoctorKafkaMonitorManager extends ModuleManager implements MonitorManager {
  private static final Logger LOG = LogManager.getLogger(DoctorKafkaMonitorManager.class);

  public DoctorKafkaMonitorManager(Configuration config){
    super(config);
  }

  @Override
  public Monitor getMonitor(String name, AbstractConfiguration additionalConfigs) throws Exception {
    Monitor monitor;
    try {
      Class<?> clazz = getModuleClass(name);
      Configuration config = getDefaultConfig(name);
      monitor = (Monitor) getConfiguredModule(clazz, additionalConfigs, config);
    } catch (Exception e){
      LOG.error("Failed to load monitor module {}. ", name, e);
      throw e;
    }
    return monitor;
  }
}
