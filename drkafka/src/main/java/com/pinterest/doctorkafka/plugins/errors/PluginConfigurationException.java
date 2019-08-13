package com.pinterest.doctorkafka.plugins.errors;

public class PluginConfigurationException extends Exception {
  public PluginConfigurationException() {
    super();
  }
  public PluginConfigurationException(String message) {
    super(message);
  }

  public PluginConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public PluginConfigurationException(Throwable cause) {
    super(cause);
  }
}
