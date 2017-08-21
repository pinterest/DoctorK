package com.pinterest.doctorkafka.stats;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

public class KafkaMetricRetrievingTask implements Callable<KafkaMetricValue> {

  private static final Logger LOG = LogManager.getLogger(KafkaMetricRetrievingTask.class);

  private MBeanServerConnection mbs;
  private String metricName;
  private String attributeName;

  public KafkaMetricRetrievingTask(MBeanServerConnection mbs,
                                   String metricName, String attributeName) {
    this.mbs = mbs;
    this.metricName = metricName;
    this.attributeName = attributeName;
  }


  @Override
  public KafkaMetricValue call() throws Exception {
    try {
      Object obj = mbs.getAttribute(new ObjectName(metricName), attributeName);
      return new KafkaMetricValue(obj);
    } catch (Exception e)  {
      return new KafkaMetricValue(e);
    }
  }
}
