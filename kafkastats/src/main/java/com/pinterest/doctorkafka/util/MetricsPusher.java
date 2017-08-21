package com.pinterest.doctorkafka.util;

import com.twitter.ostrich.stats.Distribution;
import com.twitter.ostrich.stats.Stats$;
import com.twitter.ostrich.stats.StatsListener;
import com.twitter.ostrich.stats.StatsSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;

import java.net.UnknownHostException;

/**
 * A daemon thread that periodically reads stats from Ostrich and sends them to OpenTSDB.
 *
 * The thread polls Ostrich every N milliseconds to get the counter, gauge and metric values since
 * the last interval (the very first interval is discarded, since its start time is unknown). It is
 * important that the intervals be as close to each other as possible (so they can be compared to
 * each other), so every effort is made to keep the intervals identical (but this isn't a real-time
 * system, so there are no guarantees).
 *
 * A {@link OpenTsdbMetricConverter} is used to convert from Ostrich stats to OpenTSDB stats. The
 * converter can be used to rename stats, add tags and even to modify the stat value. In addition,
 * the logic for converting Ostrich {@link Distribution}s to OpenTSDB stats must be done inside an
 * {@link OpenTsdbMetricConverter}.
 */
public class MetricsPusher extends Thread {

  private final OpenTsdbMetricConverter converter;
  private final long pollMillis;
  private final OpenTsdbClient.MetricsBuffer buffer;
  private final String host;
  private final int port;
  private OpenTsdbClient client;
  private StatsListener statsListener;

  private static final Logger LOG = LoggerFactory.getLogger(MetricsPusher.class);
  private static final int RETRY_SLEEP_MS = 100;
  private static final int MIN_SOCKET_TIME_MS = 200;

  public MetricsPusher(
      String host, int port, OpenTsdbMetricConverter converter, long pollMillis)
      throws UnknownHostException {
    this.host = host;
    this.port = port;
    this.converter = converter;
    this.pollMillis = pollMillis;
    this.buffer = new OpenTsdbClient.MetricsBuffer();

    this.client = new OpenTsdbClient(host, port);
    this.statsListener = new StatsListener(Stats$.MODULE$);
    setDaemon(true);
  }

  private void fillMetricsBuffer(StatsSummary summary, int epochSecs) {
    buffer.reset();
    OpenTsdbClient.MetricsBuffer buf = buffer;

    Map<String, Long> counters = (Map<String, Long>) (Map<String, ?>) summary.counters();
    Iterator<Tuple2<String, Long>> countersIter = counters.iterator();
    while (countersIter.hasNext()) {
      Tuple2<String, Long> tuple = countersIter.next();
      converter.convertCounter(tuple._1(), epochSecs, tuple._2(), buf);
    }

    Map<String, Double> gauges = (Map<String, Double>) (Map<String, ?>) summary.gauges();
    Iterator<Tuple2<String, Double>> gaugesIter = gauges.iterator();
    while (gaugesIter.hasNext()) {
      Tuple2<String, Double> tuple = gaugesIter.next();
      converter.convertGauge(tuple._1(), epochSecs, (float) tuple._2().doubleValue(), buf);
    }

    Map<String, Distribution> metrics = summary.metrics();
    Iterator<Tuple2<String, Distribution>> metricsIter = metrics.iterator();
    while (metricsIter.hasNext()) {
      Tuple2<String, Distribution> tuple = metricsIter.next();
      converter.convertMetric(tuple._1(), epochSecs, tuple._2(), buf);
    }
  }

  private void logOstrichStats(int epochSecs) {
    LOG.debug("Ostrich Metrics {}: \n{}", epochSecs, buffer.toString());
  }

  public long sendMetrics(boolean retryOnFailure)
      throws InterruptedException, UnknownHostException {
    long startTimeinMillis = System.currentTimeMillis();
    long end = startTimeinMillis + pollMillis;

    StatsSummary summary = statsListener.get();
    int epochSecs = (int) (startTimeinMillis / 1000L);
    fillMetricsBuffer(summary, epochSecs);
    if (LOG.isDebugEnabled()) {
      logOstrichStats(epochSecs);
    }

    while (true) {
      try {
        client.sendMetrics(buffer);
        break;
      } catch (Exception ex) {
        LOG.warn("Failed to send stats to OpenTSDB, will retry up to next interval", ex);
        if (!retryOnFailure) {
          break;
        }
        // re-initiaize OpenTsdbClient before retrying
        client = new OpenTsdbClient(host, port);
      }
      if (end - System.currentTimeMillis() < RETRY_SLEEP_MS + MIN_SOCKET_TIME_MS) {
        LOG.error("Failed to send epoch {} to OpenTSDB, moving to next interval", epochSecs);
        break;
      }
      Thread.sleep(RETRY_SLEEP_MS);
    }

    return System.currentTimeMillis() - startTimeinMillis;
  }

  @Override
  public void run() {
    try {
      // Ignore the first interval, since we don't know when stats started being recorded,
      // and we want to make sure all intervals are roughly the same length.
      statsListener.get();
      Thread.sleep(pollMillis);
      while (!Thread.currentThread().isInterrupted()) {
        long elapsedTimeMillis = sendMetrics(true);
        Thread.sleep(Math.max(0, pollMillis - elapsedTimeMillis));
      }
    } catch (InterruptedException ex) {
      LOG.info("OpenTsdbMetricsPusher thread interrupted, exiting");
    } catch (Exception ex) {
      LOG.error("Unexpected error in OpenTSDBMetricsPusher, exiting", ex);
    }
  }
}
