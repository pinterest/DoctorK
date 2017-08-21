package com.pinterest.doctorkafka.stats;

import com.pinterest.doctorkafka.ReplicaStat;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.management.MBeanServerConnection;


public class MetricsRetriever {

  private static final int METRIC_COLLECTOR_THREADPOOL_SIZE = 40;

  private static ExecutorService metricsThreadPool = Executors.newFixedThreadPool(
      METRIC_COLLECTOR_THREADPOOL_SIZE,
      new ThreadFactoryBuilder().setNameFormat("MetricsRetriever: %d").build());

  public static Future<KafkaMetricValue> getMetricValue(MBeanServerConnection mbs,
                                                        String metricName, String attributeName) {
    Callable<KafkaMetricValue> task =
        new KafkaMetricRetrievingTask(mbs, metricName, attributeName);
    Future<KafkaMetricValue> metricFuture = metricsThreadPool.submit(task);
    return metricFuture;
  }

  public static Future<ReplicaStat> getTopicPartitionReplicaStats(MBeanServerConnection mbs,
                                                                  TopicPartition topicPartition,
                                                                  boolean isLeader,
                                                                  boolean inReassign) {
    Callable<ReplicaStat> task = new ReplicaStatsTask(mbs, topicPartition, isLeader, inReassign);
    Future<ReplicaStat> statsFuture = metricsThreadPool.submit(task);
    return statsFuture;
  }
}
