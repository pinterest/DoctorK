package com.pinterest.doctorkafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DoctorKafkaWatcher implements  Runnable {
  private static final Logger LOG = LogManager.getLogger(DoctorKafkaWatcher.class);
  private static final int INITIAL_DELAY = 0;
  /**
   * The executor service for executing MercedWorkerMonitor thread
   */
  public static ScheduledExecutorService monitorExecutor;

  static {
    monitorExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("Monitor").build());
  }

  private long restartTime;

  public DoctorKafkaWatcher(long uptimeInSeconds) {
    this.restartTime = System.currentTimeMillis() + uptimeInSeconds * 1000L;
  }


  public void start() {
    monitorExecutor.scheduleAtFixedRate(this, INITIAL_DELAY, 15, TimeUnit.SECONDS);
  }

  public void stop() throws Exception {
    monitorExecutor.shutdown();
  }

  @Override
  public void run() {
    long now = System.currentTimeMillis();

    if (now > restartTime) {
      LOG.warn("Restarting metrics collector");
      System.exit(0);
    }
  }
}
