package com.pinterest.doctork.plugins.errors;

public class PluginException extends Exception {
  public PluginException() {
    super();
  }
  public PluginException(String message) {
    super(message);
  }

  public PluginException(String message, Throwable cause) {
    super(message, cause);
  }

  public PluginException(Throwable cause) {
    super(cause);
  }
}
