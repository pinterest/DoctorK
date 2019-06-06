package com.pinterest.doctorkafka.modules.action;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.event.Event;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

public class SendEmail extends Action {

  private static final Logger LOG = LogManager.getLogger(SendEmail.class);
  private static final String TITLE_PREFIX = "doctorkafka : ";
  private static final String TMP_FILE_PREFIX = "/tmp/doctorkafka_";

  private static final String CONFIG_EMAILS_KEY = "emails";

  private String[] configEmails;

  private static final String EVENT_TITLE_KEY = "title";
  private static final String EVENT_MESSAGE_KEY = "message";

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
    if(!config.containsKey(CONFIG_EMAILS_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_EMAILS_KEY + " in action " + SendEmail.class);
    }
    configEmails = config.getStringArray(CONFIG_EMAILS_KEY);
  }

  @Override
  public Collection<Event> execute(Event event) throws Exception {
    if(isDryRun()) {
      LOG.info("Dry run: Action {} triggered by event {}", this.getClass(), event.getName());
    } else if(event.containsAttribute(EVENT_TITLE_KEY) && event.containsAttribute(EVENT_MESSAGE_KEY)) {
      String title = (String) event.getAttribute(EVENT_TITLE_KEY);
      String message = (String) event.getAttribute(EVENT_MESSAGE_KEY);
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
      LOG.error("Failed to send email to {}, title: {}, body:{}", emails, title, content);
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
        LOG.error("Interrupted in sending mail to {} : {}:{}", email, title, content);
      }
    }
    File file = new File(tmpFileName);
    file.delete();
  }
}
