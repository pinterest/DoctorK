package com.pinterest.doctorkafka.plugins.task.cluster;

import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.task.Task;

import org.apache.commons.configuration2.ImmutableConfiguration;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This action sends emails to a list of email addresses, but with a cooldown time for each type of task
 *
 * <pre>
 * config:
 * [required]
 *   emails: <comma separated list of email addresses to send >
 * [optional]
 *   snooze_seconds: <snooze time for each task. Default: 20 minutes >
 *
 * Input Task Format:
 * {
 *    title: str,
 *    message: str
 * }
 * </pre>
 */
public class SnoozedSendEmailTaskHandler extends SendEmailTaskHandler {
  
  private static final String CONFIG_SNOOZE_SECONDS_KEY = "snooze_seconds";

  private long configSnoozeSeconds = 1200L;

  private Map<String, Long> prevSentTimePerTask = new ConcurrentHashMap<>();

  @Override
  public void configure(ImmutableConfiguration config) throws PluginConfigurationException {
    configSnoozeSeconds = config.getLong(CONFIG_SNOOZE_SECONDS_KEY, configSnoozeSeconds);
  }

  @Override
  public Collection<Task> execute(Task task) throws Exception {
    String taskName = task.getName();
    if (hasSnoozeExpired(taskName)){
      prevSentTimePerTask.put(taskName, System.currentTimeMillis());
      return execute(task);
    }
    return null;
  }

  protected boolean hasSnoozeExpired(String taskName) {
    long prevSentTime = prevSentTimePerTask.computeIfAbsent(taskName, k -> -1L);
    return ( System.currentTimeMillis() - prevSentTime ) > configSnoozeSeconds * 1000L;
  }
}
