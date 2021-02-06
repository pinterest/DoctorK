package com.pinterest.doctork.stats;

public class KafkaMetricValue {

  public final Object  value;
  public final Exception exception;

  public KafkaMetricValue(Object value) {
    this(value, null);
  }

  public KafkaMetricValue(Exception e) {
    this(null, e);
  }

  public KafkaMetricValue(Object value, Exception e){
    this.value = value;
    this.exception = e;
  }

  public boolean getException() {
    return exception != null;
  }

  public double toDouble() {
    return (Double)value;
  }

  public long toLong() {
    if (value instanceof Double) {
      return ((Double)value).longValue();
    }
    return (Long)value;
  }

  public int toInteger() {
    return (Integer)value;
  }
}
