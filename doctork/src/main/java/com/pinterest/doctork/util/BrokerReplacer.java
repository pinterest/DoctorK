package com.pinterest.doctork.util;

import com.pinterest.doctork.KafkaClusterManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class BrokerReplacer implements Runnable {

  private static final Logger LOG = LogManager.getLogger(KafkaClusterManager.class);

  private String script;
  private boolean inBrokerReplacement;
  private String broker;
  private long replacementStartTime;
  private Thread thread;

  public BrokerReplacer(String script) {
    this.script = script;
    this.inBrokerReplacement = false;
    this.replacementStartTime = -1;
  }

  public void replaceBroker(String brokerName) {
    if (!inBrokerReplacement) {
      this.broker = brokerName;
      this.inBrokerReplacement = true;
      thread = new Thread(this);
      thread.start();
    }
  }

  public boolean busy() {
    return this.inBrokerReplacement;
  }

  public long getReplacementStartTime() {
    return replacementStartTime;
  }

  public String getReplacedBroker() {
    return broker;
  }

  public void abort() {
    if (inBrokerReplacement) {
      thread.interrupt();
    }
  }

  @Override
  public void run() {
    inBrokerReplacement = true;
    replacementStartTime = System.currentTimeMillis();
    String[] replaceBrokerCommand = new String[3];
    replaceBrokerCommand[0] = "/bin/sh";
    replaceBrokerCommand[1] = "-c";
    replaceBrokerCommand[2] = script + " " + broker;
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
      inBrokerReplacement = false;
    }
  }
}
