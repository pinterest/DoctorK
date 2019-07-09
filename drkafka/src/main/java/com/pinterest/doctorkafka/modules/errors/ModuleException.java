package com.pinterest.doctorkafka.modules.errors;

public class ModuleException extends Exception {
  public ModuleException() {
    super();
  }
  public ModuleException(String message) {
    super(message);
  }

  public ModuleException(String message, Throwable cause) {
    super(message, cause);
  }

  public ModuleException(Throwable cause) {
    super(cause);
  }
}
