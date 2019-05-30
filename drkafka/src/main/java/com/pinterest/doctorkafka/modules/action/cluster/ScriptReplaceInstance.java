package com.pinterest.doctorkafka.modules.action.cluster;

import com.pinterest.doctorkafka.modules.action.errors.ProlongReplacementException;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ScriptReplaceInstance implements ReplaceInstance, Runnable {
  private static final Logger LOG = LogManager.getLogger(ScriptReplaceInstance.class);

  private static final String CONFIG_SCRIPT_KEY = "script";
  private static final String CONFIG_PROLONG_REPLACEMENT_ALERT_SECONDS_KEY = "prolong.replacement.alert.seconds";

  private static final long DEFAULT_PROLONG_REPLACEMENT_ALERT_SECONDS = 1800L;

  private String configScript;
  private long configProlongReplacementAlertSeconds;

  private volatile boolean isBusy = false;
  private String hostname;
  private long replacementStartTime = -1;
  private Thread thread;

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    if (!config.containsKey(CONFIG_SCRIPT_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_SCRIPT_KEY + " for plugin " + this.getClass());
    }
    this.configScript = config.getString(CONFIG_SCRIPT_KEY);
    this.configProlongReplacementAlertSeconds = config.getLong(
        CONFIG_PROLONG_REPLACEMENT_ALERT_SECONDS_KEY,
        DEFAULT_PROLONG_REPLACEMENT_ALERT_SECONDS
    );
  }

  public void replace(String hostname) throws Exception {
    long now = System.currentTimeMillis();
    if (!isBusy) {
      this.hostname = hostname;
      isBusy = true;
      replacementStartTime = now;
      thread = new Thread(this);
      thread.start();
    } else {
      long replacementTime = (now - replacementStartTime)/1000L;
      LOG.info("Cannot replace broker {}: Busy replacing {}", hostname, this.hostname);
      if (replacementTime > configProlongReplacementAlertSeconds) {
        throw new ProlongReplacementException(
            "Replacement of instance " + this.hostname +
                " has not finished after " + replacementTime + " seconds",
            this.hostname
        );
      }
    }
  }

  public boolean isBusy(){
    return isBusy;
  }

  @Override
  public void run() {
    String[] replaceBrokerCommand = new String[3];
    replaceBrokerCommand[0] = "/bin/sh";
    replaceBrokerCommand[1] = "-c";
    replaceBrokerCommand[2] = configScript + " " + hostname;
    LOG.info("Broker replacement command : " + replaceBrokerCommand[0] + " "
        + replaceBrokerCommand[1] + replaceBrokerCommand[2]);

    try {
      LOG.info("starting broker replacement process");
      Process process = Runtime.getRuntime().exec(replaceBrokerCommand);
      process.isAlive();
      process.waitFor();
      InputStream inputStream = process.getInputStream();
      BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while ((line = in.readLine()) != null) {
        LOG.info(line);
      }
      in.close();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      LOG.error("Failure in broker replacement", e);
    } finally {
      isBusy = false;
    }
  }
}
