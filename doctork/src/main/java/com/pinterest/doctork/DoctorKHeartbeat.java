package com.pinterest.doctork;

import com.pinterest.doctork.util.OpenTsdbMetricConverter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DoctorKHeartbeat implements Runnable {
  private static final int HEARTBEAT_INTERVAL_IN_SECONDS = 60;
  public ScheduledExecutorService heartbeatExecutor;

  DoctorKHeartbeat() {
    heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("Heartbeat").build());
  }

  public void start() {
    heartbeatExecutor.scheduleAtFixedRate(this, 0, HEARTBEAT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
  }

  public void stop() {
    heartbeatExecutor.shutdown();
  }

  @Override
  public void run() {
    OpenTsdbMetricConverter.gauge(DoctorKMetrics.DOCTORK_SERVICE_RUNNING, 1.0);
  }
}
