package com.pinterest.doctorkafka.modules.action.errors;

public class ProlongReplacementException extends Exception {
  private String hostname;
  public ProlongReplacementException(String msg, String hostname) {
    super(msg);
    this.hostname = hostname;
  }

  public ProlongReplacementException(String msg, Throwable cause, String hostname) {
    super(msg, cause);
    this.hostname = hostname;
  }

  public ProlongReplacementException(Throwable cause, String hostname) {
    super(cause);
    this.hostname = hostname;
  }

  public String getHostname() {
    return hostname;
  }
}
