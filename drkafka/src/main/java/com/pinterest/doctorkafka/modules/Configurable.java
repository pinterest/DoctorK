package com.pinterest.doctorkafka.modules;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;

/**
 * An Configurable is a module that performs operations on a Context.
 * All configuration should be done once the module is initialized
 */

public interface Configurable {
  default void configure(AbstractConfiguration config) throws ModuleConfigurationException {}
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
