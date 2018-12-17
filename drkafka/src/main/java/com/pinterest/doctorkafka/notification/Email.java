package com.pinterest.doctorkafka.notification;

import com.pinterest.doctorkafka.KafkaBroker;

import kafka.cluster.Broker;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Email {

  private static final Logger LOG = LogManager.getLogger(Email.class);
  private static final String TITLE_PREFIX = "doctorkafka : ";
  private static final String TMP_FILE_PREFIX = "/tmp/doctorkafka_";
  private static final long COOLOFF_INTERVAL = 1200000L;

  private static final Map<String, Long> reassignmentEmails = new ConcurrentHashMap<>();
  private static final Map<String, Long> urpFailureEmails = new ConcurrentHashMap<>();
  private static final Map<String, Long> prolongedUrpEmails = new ConcurrentHashMap<>();
  private static final Map<String, Long> noStatsBrokerEmails = new ConcurrentHashMap<>();
  private static final Map<String, Long> slowBrokerReplacementEmail = new ConcurrentHashMap<>();


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


  public static void notifyOnPartitionReassignment(String[] emails,
                                                   String clusterName,
                                                   String assignmentJson) {
    if (reassignmentEmails.containsKey(clusterName) &&
        System.currentTimeMillis() - reassignmentEmails.get(clusterName) < COOLOFF_INTERVAL) {
      // return to avoid spamming users if an email has been sent within the coll-time time span
      return;
    }

    reassignmentEmails.put(clusterName, System.currentTimeMillis());
    String title = clusterName + " partition reassignment ";
    String content = "Assignment json: \n\n" + assignmentJson;
    sendTo(emails, title, content);
  }

  public static void notifyOnBrokerReplacement(String[] emails, String clusterName, String broker) {
    String title = clusterName + " broker replacement : " + broker;
    String content = "Replacing broker " + broker + " in cluster " + clusterName;
    sendTo(emails, title, content);
  }
  
  public static void notifyOnMaintenanceMode(String[] emails, String clusterName, boolean maintenanceMode) {
    String title = maintenanceMode ? clusterName + " is in Maintenance mode" : " is out of Maintenance mode";
    String content = "Cluster:" + clusterName + " was placed " + (maintenanceMode ? "in maintenance mode"
        : "out of maintenance mode") + " at " + new Date();
    sendTo(emails, title, content);
  }

  public static void alertOnNoStatsBrokers(String[] emails,
                                           String clusterName,
                                           List<Broker> noStatsBrokers) {

    if (noStatsBrokerEmails.containsKey(clusterName) &&
        System.currentTimeMillis() - noStatsBrokerEmails.get(clusterName) < COOLOFF_INTERVAL) {
      return;
    }
    noStatsBrokerEmails.put(clusterName, System.currentTimeMillis());
    String title = clusterName + " : " + noStatsBrokers.size() + " brokers do not have stats";
    StringBuilder sb = new StringBuilder();
    sb.append("No stats brokers : \n");
    noStatsBrokers.stream().forEach(broker -> sb.append(broker + "\n"));
    sendTo(emails, title, sb.toString());
  }

  public static void alertOnFailureInHandlingUrps(String[] emails,
                                                  String clusterName,
                                                  List<PartitionInfo> urps,
                                                  List<MutablePair<KafkaBroker, TopicPartition>>
                                                      reassignmentFailures,
                                                  Set<Integer> downBrokers) {
    if (urpFailureEmails.containsKey(clusterName) &&
        System.currentTimeMillis() - urpFailureEmails.get(clusterName) < COOLOFF_INTERVAL) {
      // return to avoid spamming users if an email has been sent within the coll-time time span
      return;
    }

    urpFailureEmails.put(clusterName, System.currentTimeMillis());
    String title = "Failed to handle under-replicated partitions on " + clusterName
        + " (" + urps.size() + " under-replicated partitions)";
    StringBuilder sb = new StringBuilder();
    for (PartitionInfo partitionInfo : urps) {
      sb.append(partitionInfo + "\n");
    }
    if (reassignmentFailures != null && !reassignmentFailures.isEmpty()) {
      sb.append("Reassignment failure: \n");
      reassignmentFailures.stream().forEach(pair -> {
        KafkaBroker broker = pair.getKey();
        TopicPartition topicPartition = pair.getValue();
        sb.append("Broker : " + broker.name() + ", " + topicPartition);
      });
    }
    if (downBrokers != null && !downBrokers.isEmpty()) {
      sb.append("Down brokers: \n");
      sb.append(downBrokers);
    }
    String content = sb.toString();
    sendTo(emails, title, content);
  }


  public static void alertOnProlongedUnderReplicatedPartitions(String[] emails,
                                                               String clusterName,
                                                               int waitTimeInSeconds,
                                                               List<PartitionInfo> urps) {
    if (prolongedUrpEmails.containsKey(clusterName) &&
        System.currentTimeMillis() - prolongedUrpEmails.get(clusterName) < COOLOFF_INTERVAL) {
      // return to avoid spamming users if an email has been sent within the coll-time time span
      return;
    }

    prolongedUrpEmails.put(clusterName, System.currentTimeMillis());
    String title = clusterName + " has been under-replicated for > "
        + waitTimeInSeconds + " seconds (" + urps.size() + ") under-replicated partitions";
    StringBuilder sb = new StringBuilder();
    for (PartitionInfo partitionInfo : urps) {
      sb.append(partitionInfo + "\n");
    }
    String content = sb.toString();
    sendTo(emails, title, content);
  }


  public static void alertOnProlongedBrokerReplacement(String[] emails,
                                                       String clusterName,
                                                       String brokerName,
                                                       long replacementTimeInSeconds) {
    long lastEmailTime = System.currentTimeMillis() - slowBrokerReplacementEmail.get(clusterName);
    if (slowBrokerReplacementEmail.containsKey(clusterName) && lastEmailTime < COOLOFF_INTERVAL) {
      // return to avoid spamming users with too many emails
      return;
    }

    slowBrokerReplacementEmail.put(clusterName, System.currentTimeMillis());
    String title = "Slow broker replacement : " + clusterName + ", " + brokerName;
    StringBuilder sb = new StringBuilder();
    sb.append("Broker replacement " + clusterName + ":" + brokerName
        + " has not finished after " + replacementTimeInSeconds + " seconds");
    sendTo(emails, title, sb.toString());
  }
}
