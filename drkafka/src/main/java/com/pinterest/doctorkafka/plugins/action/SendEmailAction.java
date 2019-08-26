package com.pinterest.doctorkafka.plugins.action;

import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.context.event.Event;
import com.pinterest.doctorkafka.plugins.context.event.EventUtils;

import org.apache.commons.configuration2.AbstractConfiguration;
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
 * Input Event Format:
 * {
 *   title: str,
 *   message: str
 * }
 * </pre>
 */
public class SendEmailAction extends Action {

  private static final Logger LOG = LogManager.getLogger(SendEmailAction.class);
  private static final String TITLE_PREFIX = "doctorkafka : ";
  private static final String TMP_FILE_PREFIX = "/tmp/doctorkafka_";

  private static final String CONFIG_EMAILS_KEY = "emails";

  private String[] configEmails;

  @Override
  public void configure(AbstractConfiguration config) throws PluginConfigurationException {
    super.configure(config);
    if(!config.containsKey(CONFIG_EMAILS_KEY)){
      throw new PluginConfigurationException("Missing config " + CONFIG_EMAILS_KEY + " in action " + SendEmailAction.class);
    }
    configEmails = config.getString(CONFIG_EMAILS_KEY).split(",");
  }

  @Override
  public Collection<Event> execute(Event event) throws Exception {
    if(event.containsAttribute(EventUtils.EVENT_TITLE_KEY) && event.containsAttribute(EventUtils.EVENT_MESSAGE_KEY)) {
      String title = (String) event.getAttribute(EventUtils.EVENT_TITLE_KEY);
      String message = (String) event.getAttribute(EventUtils.EVENT_MESSAGE_KEY);
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
