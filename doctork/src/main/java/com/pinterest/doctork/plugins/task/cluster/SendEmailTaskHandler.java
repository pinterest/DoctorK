package com.pinterest.doctork.plugins.task.cluster;

import com.pinterest.doctork.plugins.errors.PluginConfigurationException;
import com.pinterest.doctork.plugins.task.Task;
import com.pinterest.doctork.plugins.task.TaskHandler;
import com.pinterest.doctork.plugins.task.TaskUtils;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

/**
 * This action sends emails to a list of email addresses
 *
 * <pre>
 * config:
 * [required]
 *   emails: < comma separated list of email addresses to send >
 *
 * Input Task Format:
 * {
 *   title: str,
 *   message: str
 * }
 * </pre>
 */
public class SendEmailTaskHandler extends TaskHandler {

  private static final Logger LOG = LogManager.getLogger(SendEmailTaskHandler.class);
  private static final String TITLE_PREFIX = "doctork : ";
  private static final String TMP_FILE_PREFIX = "/tmp/doctork_";

  private static final String CONFIG_EMAILS_KEY = "emails";

  private String[] configEmails;

  @Override
  public void configure(ImmutableConfiguration config) throws PluginConfigurationException {
    if(!config.containsKey(CONFIG_EMAILS_KEY)){
      throw new PluginConfigurationException("Missing config " + CONFIG_EMAILS_KEY + " in action " + SendEmailTaskHandler.class);
    }
    configEmails = config.getString(CONFIG_EMAILS_KEY).split(",");
  }

  @Override
  public Collection<Task> execute(Task task) throws Exception {
    if(task.containsAttribute(TaskUtils.TASK_TITLE_KEY) && task.containsAttribute(TaskUtils.TASK_MESSAGE_KEY)) {
      String title = (String) task.getAttribute(TaskUtils.TASK_TITLE_KEY);
      String message = (String) task.getAttribute(TaskUtils.TASK_MESSAGE_KEY);
      sendTo(configEmails,title, message);
    }
    return null;
  }

  public static void sendTo(String[] emails, String title, String content) {
    String tmpFileName = TMP_FILE_PREFIX + "_" + System.currentTimeMillis() + ".txt";
    try {
      PrintWriter writer = new PrintWriter(tmpFileName, "UTF-8");
      writer.println(content);
      writer.close();
    } catch (IOException e) {
      LOG.error("Failed to send email to {}, title: {}, body:{}", emails, title, content, e);
    }

    title = TITLE_PREFIX + title;

    for (String email : emails) {
      String[] cmd = {"/bin/sh", "-c",
                      "mail -s \"" + title + "\" " + email + " < " + tmpFileName};
      try {
        Process p = Runtime.getRuntime().exec(cmd);
        synchronized (p) {
          p.wait();
        }
      } catch (InterruptedException | IOException e) {
        LOG.error("Interrupted in sending mail to {} : {}:{}", email, title, content, e);
      }
    }
    File file = new File(tmpFileName);
    file.delete();
  }
}
