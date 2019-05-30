package com.pinterest.doctorkafka.modules.action;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class EmailSendEvent implements SendEvent {

  private static final Logger LOG = LogManager.getLogger(EmailSendEvent.class);
  private static final String TITLE_PREFIX = "doctorkafka : ";
  private static final String TMP_FILE_PREFIX = "/tmp/doctorkafka_";

  private static final String CONFIG_NOTIFY_EMAILS_KEY = "notification.emails";
  private static final String CONFIG_ALERT_EMAILS_KEY = "alert.emails";

  private String[] configNotifyEmails;
  private String[] configAlertEmails;

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    if(!config.containsKey(CONFIG_NOTIFY_EMAILS_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_NOTIFY_EMAILS_KEY + " in plugin " + EmailSendEvent.class);
    }
    configNotifyEmails = config.getStringArray(CONFIG_NOTIFY_EMAILS_KEY);
    if(!config.containsKey(CONFIG_ALERT_EMAILS_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_ALERT_EMAILS_KEY + " in plugin " + EmailSendEvent.class);
    }
    configAlertEmails = config.getStringArray(CONFIG_ALERT_EMAILS_KEY);
  }

  @Override
  public void notify(String title, String message) {
    sendTo(configNotifyEmails,title, message);
  }
  @Override
  public void alert(String title, String message) {
    sendTo(configAlertEmails,title, message);
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
