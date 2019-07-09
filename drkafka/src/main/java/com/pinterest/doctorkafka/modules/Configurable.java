package com.pinterest.doctorkafka.modules;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;

/**
 * Configurable is the base class of all Doctorkafka modules.
 * All configuration should be done right after the module has been initialized
 */

public interface Configurable {

  /**
   * Apply configurations from a single config
   * @param config
   * @throws ModuleConfigurationException
   */
  default void configure(AbstractConfiguration config) throws ModuleConfigurationException {}

  /**
   * Combines multiple configs into one config and call the single config Configure method
   * @param configurations
   * @throws ModuleConfigurationException
   */
  default void configure(Configuration... configurations) throws ModuleConfigurationException {
    CompositeConfiguration compositeConfiguration = new CompositeConfiguration();
    for (Configuration config : configurations) {
      if (config != null){
        compositeConfiguration.addConfiguration(config);
      }
    }
    configure(compositeConfiguration);
  }
}
