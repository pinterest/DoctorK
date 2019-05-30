package com.pinterest.doctorkafka.modules.action.errors;

public class ExistingReassignmentException extends Exception {
  public ExistingReassignmentException(String msg) {
    super(msg);
  }

  public ExistingReassignmentException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public ExistingReassignmentException(Throwable cause) {
    super(cause);
  }
}
