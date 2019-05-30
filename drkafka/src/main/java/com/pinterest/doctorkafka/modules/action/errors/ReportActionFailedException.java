package com.pinterest.doctorkafka.modules.action.errors;

public class ReportActionFailedException extends Exception {
  public ReportActionFailedException(String msg) {
    super(msg);
  }

  public ReportActionFailedException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public ReportActionFailedException(Throwable cause) {
    super(cause);
  }
}
