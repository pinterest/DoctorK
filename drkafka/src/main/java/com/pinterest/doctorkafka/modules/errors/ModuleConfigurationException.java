package com.pinterest.doctorkafka.modules.errors;

public class ModuleConfigurationException extends Exception {
  public ModuleConfigurationException() {
    super();
  }
  public ModuleConfigurationException(String message) {
    super(message);
  }

  public ModuleConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ModuleConfigurationException(Throwable cause) {
    super(cause);
  }
}
