package com.pinterest.doctork.plugins.task.cluster;

import com.pinterest.doctork.plugins.errors.PluginConfigurationException;
import com.pinterest.doctork.plugins.task.Task;
import com.pinterest.doctork.plugins.task.TaskHandler;
import com.pinterest.doctork.plugins.task.TaskUtils;
import com.pinterest.doctork.util.ZookeeperClient;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This action runs a script on a different thread to replace instances
 *
 * <pre>
 * config:
 * [required]
 *    script: <path to replacement script that takes hostname as the first argument>
 * [optional]
 *    prolong_replacement_alert_seconds: <time in seconds before alerting on prolong previous replacement>
 *
 * Input Task Format:
 * {
 *   cluster_name: str (Default: "n/a"),
 *   hostname: str,
 *   zookeeper_client: com.pinterest.doctork.util.ZookeeperClient
 * }
 *
 * Output Tasks Format:
 *
 * Task: notify_prolong_replacement
 * triggered when replacement has taken more than configured time
 * {
 *   title: str,
 *   message: str
 * }
 *
 * Task: notify_replacement:
 * triggered when replacement is kicked off
 * {
 *   title: str,
 *   message: str
 * }
 *
 * Task: report_operation:
 * triggered when replacement is kicked off
 * {
 *   subject: str,
 *   message: str
 * }
 * </pre>
 */
public class ScriptReplaceInstanceTaskHandler extends TaskHandler implements Runnable {
  
  private static final Logger LOG = LogManager.getLogger(ScriptReplaceInstanceTaskHandler.class);

  private static final String CONFIG_SCRIPT_KEY = "script";
  private static final String CONFIG_PROLONG_REPLACEMENT_ALERT_SECONDS_KEY = "prolong_replacement_alert_seconds";

  private String configScript;
  private long configProlongReplacementAlertSeconds = 1800L;

  private static final String TASK_NOTIFY_PROLONG_REPLACEMENT_NAME = "notify_prolong_replacement";
  private static final String TASK_NOTIFY_REPLACEMENT_NAME = "notify_replacement";
  private static final String TASK_REPORT_OPERATION_NAME = "report_operation";

  private static final String TASK_HOSTNAME_KEY = "hostname";
  private static final String TASK_ZOOKEEPER_CLIENT_KEY = "zookeeper_client";

  private static final String DEFAULT_TASK_CLUSTER_NAME = "n/a";

  private volatile boolean isBusy = false;
  private String currentHostname;
  private long replacementStartTime = -1;
  private Thread thread;

  @Override
  public void configure(ImmutableConfiguration config) throws PluginConfigurationException {
    if (!config.containsKey(CONFIG_SCRIPT_KEY)){
      throw new PluginConfigurationException("Missing config " + CONFIG_SCRIPT_KEY + " for plugin " + this.getClass());
    }
    this.configScript = config.getString(CONFIG_SCRIPT_KEY).replaceAll("^\"|\"$","");
    this.configProlongReplacementAlertSeconds = config.getLong(
        CONFIG_PROLONG_REPLACEMENT_ALERT_SECONDS_KEY,
        configProlongReplacementAlertSeconds
    );
  }

  @Override
  public Collection<Task> execute(Task task) throws Exception {
    if(task.containsAttribute(TASK_HOSTNAME_KEY) && task.containsAttribute(TASK_ZOOKEEPER_CLIENT_KEY)){
      String clusterName = task.containsAttribute(TaskUtils.TASK_CLUSTER_NAME_KEY) ?
                       (String) task.getAttribute(TaskUtils.TASK_CLUSTER_NAME_KEY) :
                           DEFAULT_TASK_CLUSTER_NAME;
      String hostname = (String) task.getAttribute(TASK_HOSTNAME_KEY);
      ZookeeperClient zookeeperClient = (ZookeeperClient) task.getAttribute(TASK_ZOOKEEPER_CLIENT_KEY);
      return replace(clusterName, hostname, zookeeperClient);
    }
    return null;
  }

  protected Collection<Task> replace(String clusterName, String hostname, ZookeeperClient zookeeperClient) throws Exception {
    long now = System.currentTimeMillis();
    if (!isBusy) {
      zookeeperClient.recordBrokerTermination(clusterName, hostname);
      currentHostname = hostname;
      isBusy = true;
      replacementStartTime = now;
      thread = new Thread(this);
      thread.start();
      return createReplacementTasks(clusterName, hostname);
    } else {
      long replacementTime = (System.currentTimeMillis() - replacementStartTime)/1000L;
      LOG.info("Cannot replace broker {}: Busy replacing {}", hostname, currentHostname);
      return checkAndCreateNotificationForProlongedReplacement(clusterName, replacementTime);
    }
  }

  @Override
  public void run() {
    String[] replaceBrokerCommand = new String[3];
    replaceBrokerCommand[0] = "/bin/sh";
    replaceBrokerCommand[1] = "-c";
    replaceBrokerCommand[2] = configScript + " " + currentHostname;
    LOG.info("Broker replacement command : " + replaceBrokerCommand[0] + " "
        + replaceBrokerCommand[1] + " " + replaceBrokerCommand[2]);

    try {
      LOG.info("starting broker replacement process");
      Process process = Runtime.getRuntime().exec(replaceBrokerCommand);
      process.waitFor();
      InputStream inputStream = process.getInputStream();
      InputStream errorStream = process.getErrorStream();
      BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
      BufferedReader err = new BufferedReader(new InputStreamReader(errorStream));
      String line;
      LOG.info("Broker replacement process\nSTDOUT:");
      while ((line = in.readLine()) != null) {
        LOG.info(line);
      }
      in.close();
      LOG.info("\nSTDERR:");
      while ((line = err.readLine()) != null) {
        LOG.info(line);
      }
      err.close();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      LOG.error("Failure in broker replacement", e);
    } finally {
      isBusy = false;
    }
  }

  protected Collection<Task> createReplacementTasks(String clusterName, String hostname){

    Collection<Task> tasks = new ArrayList<>();
    tasks.add(new ReportTask(TASK_REPORT_OPERATION_NAME, clusterName, "Replacing instance: " + hostname));

    String title = clusterName + " replacing instance " + hostname;
    String message = "Replacing instance " + hostname + " on cluster " + clusterName;
    tasks.add(new NotificationTask(TASK_NOTIFY_REPLACEMENT_NAME, title, message));

    return tasks;
  }

  protected Collection<Task> checkAndCreateNotificationForProlongedReplacement(String clusterName, long replacementTime){
    Collection<Task> tasks = new ArrayList<>();
    if (replacementTime > configProlongReplacementAlertSeconds) {
      String title = "Slow replacement of broker " + currentHostname + " in cluster " + clusterName;
      String message = "Replacement of instance " + currentHostname + " has not finished after " + replacementTime + " seconds";
      tasks.add(new NotificationTask(TASK_NOTIFY_PROLONG_REPLACEMENT_NAME, title, message));
    }
    return tasks;
  }
}
