package com.pinterest.doctork.plugins;

import com.pinterest.doctork.plugins.errors.PluginConfigurationException;

import org.apache.commons.configuration2.ImmutableConfiguration;

/**
 * Plugin is the base class of all DoctorK plugins.
 * All configuration will be applied to the plugins right after the plugin has been initialized
 */

public interface Plugin {

  /**
   * Apply configurations from a single config
   * @param config
   * @throws PluginConfigurationException
   */
  default void configure(ImmutableConfiguration config) throws PluginConfigurationException {}

  /**
   * A wrapper function around the configure function that is called by PluginManager to initialize the plugin instance.
   * @param config
   * @throws PluginConfigurationException
   */
  void initialize(ImmutableConfiguration config) throws PluginConfigurationException;
}
