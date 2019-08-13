package com.pinterest.doctorkafka.plugins;

import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;

/**
 * Configurable is the base class of all DoctorKafka plugins.
 * All configuration will be applied to the plugins right after the plugin has been initialized
 */

public interface Configurable {

  /**
   * Apply configurations from a single config
   * @param config
   * @throws PluginConfigurationException
   */
  default void configure(AbstractConfiguration config) throws PluginConfigurationException {}

  /**
   * Combines multiple configs into one config and call the single config Configure method
   * @param configurations
   * @throws PluginConfigurationException
   */
  default void configure(Configuration... configurations) throws PluginConfigurationException {
    CompositeConfiguration compositeConfiguration = new CompositeConfiguration();
    for (Configuration config : configurations) {
      if (config != null){
        compositeConfiguration.addConfiguration(config);
      }
    }
    configure(compositeConfiguration);
  }
}
