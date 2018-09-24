package com.pinterest.doctorkafka.util;

import com.google.common.base.Joiner;
import com.twitter.ostrich.stats.Distribution;
import com.twitter.ostrich.stats.Stats;
import scala.Tuple2;

/**
 * Converts Ostrich stats to OpenTSDB metrics.
 *
 * The converter takes a prefix and a hostname. The prefix is added to the front of every OpenTSDB
 * metric name, and the hostname is added as a "host=HOSTNAME" tag on every OpenTSDB metric.
 *
 * Ostrich stats are expected to be named like this:
 *
 *  "a.b.c.d tag1=value1 tag2=value2 ..."
 *
 * For counters and gauges these names are converted to OpenTSDB metrics like this:
 *
 *  "PREFIX.a.b.c.d host=HOSTNAME tag1=value1 tag2=value2 ..."
 *
 * For metrics these names are converted to a number of percentiles and counts:
 *
 *  "PREFIX.a.b.c.d.p50 host=HOSTNAME tag1=value1 ..."
 *  "PREFIX.a.b.c.d.p90 host=HOSTNAME tag1=value1 ..."
 *  "PREFIX.a.b.c.d.p95 host=HOSTNAME tag1=value1 ..."
 *  "PREFIX.a.b.c.d.p99 host=HOSTNAME tag1=value1 ..."
 *  "PREFIX.a.b.c.d.max host=HOSTNAME tag1=value1 ..."
 *  "PREFIX.a.b.c.d.count host=HOSTNAME tag1=value1 ..."
 *  "PREFIX.a.b.c.d.avg host=HOSTNAME tag1=value1 ..."
 *
 * The "addMetric" static function is provided to make it easier to add Ostrich metric names that
 * contain tags.
 */
public class OpenTsdbMetricConverter  {
  // According to http://opentsdb.net/docs/build/html/user_guide/writing.html
  public static final String VALID_OPENSTD_STAT_TAG_PATTERN = "[a-zA-Z0-9_./-]+";

  private final String prefix;
  private final String defaultTags;

  private static final Joiner SPACE_JOINER = Joiner.on(" ").skipNulls();

  public OpenTsdbMetricConverter(String prefix, String... defaultTags) {
    this.defaultTags = Joiner.on(" ").join(defaultTags);
    this.prefix = prefix;
  }

  public OpenTsdbMetricConverter(String prefix, String hostname) {
    this.prefix = prefix;
    this.defaultTags = "host=" + hostname;
  }

  public boolean convertCounter(String name, int epochSecs, float value, OpenTsdbClient.MetricsBuffer buffer) {
    return convertCounterOrGauge(name, epochSecs, value, buffer);
  }

  public boolean convertGauge(String name, int epochSecs, float value, OpenTsdbClient.MetricsBuffer buffer) {
    return convertCounterOrGauge(name, epochSecs, value, buffer);
  }

  private boolean convertCounterOrGauge(String name, int epochSecs, float value, OpenTsdbClient.MetricsBuffer buffer) {
    Tuple2<String, StringBuilder> nameAndTags = getNameAndTags(name);
    if (nameAndTags == null) {
      return false;
    }
    String statName = nameAndTags._1();
    StringBuilder tags = nameAndTags._2().append(" ").append(getDefaultTags());

    buffer.addMetric(statName, epochSecs, value, tags.toString());
    return true;
  }

  public boolean convertMetric(String name, int epochSecs, Distribution dist, OpenTsdbClient.MetricsBuffer buffer) {
    Tuple2<String, StringBuilder> nameAndTags = getNameAndTags(name);
    if (nameAndTags == null) {
      return false;
    }
    String statName = nameAndTags._1();
    StringBuilder tags = nameAndTags._2().append(" ").append(getDefaultTags());

    float p50 = dist.histogram().getPercentile(0.5);
    float p90 = dist.histogram().getPercentile(0.9);
    float p95 = dist.histogram().getPercentile(0.95);
    float p99 = dist.histogram().getPercentile(0.99);
    long max = dist.maximum();

    long count = dist.count();
    float avg = (float) dist.average();

    buffer.addMetric(statName + ".p50", epochSecs, p50, tags.toString());
    buffer.addMetric(statName + ".p90", epochSecs, p90, tags.toString());
    buffer.addMetric(statName + ".p95", epochSecs, p95, tags.toString());
    buffer.addMetric(statName + ".p99", epochSecs, p99, tags.toString());
    buffer.addMetric(statName + ".max", epochSecs, max, tags.toString());
    buffer.addMetric(statName + ".count", epochSecs, count, tags.toString());
    buffer.addMetric(statName + ".avg", epochSecs, avg, tags.toString());
    return true;
  }

  private String getDefaultTags() {
    return defaultTags;
  }

  private Tuple2<String, StringBuilder> getNameAndTags(String ostrichStatName) {
    String[] parts = ostrichStatName.split(" ");

    String openTsdStatName = prefix + "." + parts[0];
    if (!openTsdStatName.matches(VALID_OPENSTD_STAT_TAG_PATTERN)) {
      return null;
    }

    StringBuilder tags = new StringBuilder();
    for (int i = 1; i < parts.length; i++) {
      for (String tagPart : parts[i].split("=")) {
        if (!tagPart.matches(VALID_OPENSTD_STAT_TAG_PATTERN)) {
          return null;
        }
      }
      tags.append(" ");
      tags.append(parts[i]);
    }

    return new Tuple2<>(openTsdStatName, tags);
  }

  public static String nameMetric(String name, String... tags) {
    StringBuilder builder = new StringBuilder(name);
    builder.append(" ")
        .append(SPACE_JOINER.join(tags));
    return builder.toString();
  }

  public static void addMetric(String name, int value) {
    Stats.addMetric(name, value);
  }

  public static void addMetric(String name, int value, String... tags) {
    Stats.addMetric(nameMetric(name, tags), value);
  }

  public static void incr(String name) {
    Stats.incr(name);
  }

  public static void incr(String name, String... tags) {
    Stats.incr(nameMetric(name, tags));
  }

  public static void incr(String name, int i, String... tags) {
    Stats.incr(nameMetric(name, tags), i);
  }

  public static void gauge(String name, double value) {
    Stats.setGauge(name, value);
  }

  public static void gauge(String name, double value, String... tags) {
    Stats.setGauge(nameMetric(name, tags), value);
  }
}
