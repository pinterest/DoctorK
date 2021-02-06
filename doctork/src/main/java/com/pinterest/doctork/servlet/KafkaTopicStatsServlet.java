package com.pinterest.doctork.servlet;


import com.pinterest.doctork.DoctorKMain;
import com.pinterest.doctork.KafkaCluster;
import com.pinterest.doctork.KafkaClusterManager;
import com.pinterest.doctork.util.KafkaUtils;
import com.pinterest.doctork.errors.ClusterInfoError;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeSet;

public class KafkaTopicStatsServlet extends DoctorKServlet {

  private static final Logger LOG = LogManager.getLogger(KafkaTopicStatsServlet.class);
  private static final Gson gson = new Gson();
  
  @Override
  public void renderJSON(PrintWriter writer, Map<String, String> params) {
    String clusterName = params.get("cluster");
    String topic = params.get("topic");
    JsonArray json = new JsonArray();

    KafkaClusterManager clusterMananger =
      DoctorKMain.doctorK.getClusterManager(clusterName);
    if (clusterMananger == null) {
      ClusterInfoError error = new ClusterInfoError("Failed to find cluster manager for {}", clusterName);
      writer.print(gson.toJson(error));
      return;
    }

    try {
      KafkaCluster cluster = clusterMananger.getCluster();
      
      TreeSet<TopicPartition> topicPartitions =
        new TreeSet( new KafkaUtils.TopicPartitionComparator());
      topicPartitions.addAll(cluster.topicPartitions.get(topic));

      for (TopicPartition topicPartition : topicPartitions) {
	double bytesInMax =
          cluster.getMaxBytesIn(topicPartition) / 1024.0 / 1024.0;
	double bytesOutMax =
            cluster.getMaxBytesOut(topicPartition) / 1024.0 / 1024.0;

	JsonObject jsonPartition = new JsonObject();
	jsonPartition.add("bytesInMax", gson.toJsonTree(bytesInMax));
	jsonPartition.add("bytesOutMax", gson.toJsonTree(bytesOutMax));
	jsonPartition.add("partition", gson.toJsonTree(topicPartition.partition()));
	json.add(jsonPartition);
      }
      writer.print(json);
    } catch (Exception e) {
      writer.print(gson.toJson(e));
    }
  }

  @Override
  public void renderHTML(PrintWriter writer, Map<String, String> params) {
    String clusterName = params.get("cluster");
    String topic = params.get("topic");
    try {
      printHeader(writer);
      writer.print("<div> <p><a href=\"/\">Home</a> > "
          + "<a href=\"/servlet/clusterinfo?name=" + clusterName + "\"> " + clusterName
          + "</a> > " + topic + "</p> </div>");

      // generate the HTML markups
      KafkaClusterManager clusterMananger =
          DoctorKMain.doctorK.getClusterManager(clusterName);
      if (clusterMananger == null) {
        writer.print("Failed to find cluster manager for " + clusterName);
        return;
      }
      writer.print("<div> <h4> Cluster : " + clusterName + "</h4> </div>");
      KafkaCluster cluster = clusterMananger.getCluster();
      printTopicPartitionInfo(cluster, writer, topic);
      printFooter(writer);

    } catch (Exception e) {
      e.printStackTrace(writer);
    }
  }

  private void printTopicPartitionInfo(KafkaCluster cluster, PrintWriter writer, String topic) {

    writer.print("<p> <h5>" + topic + "</h5></p>");
    writer.print("<table class=\"table\">");
    writer.print("<thead> <tr> <th> Partition</th> ");
    writer.print("<th>In Max (Mb/s)</th> ");
    writer.print("<th>Out max (Mb/s)</th>");
    writer.print("</tr> </thead> <tbody>");

    int zeroTrafficPartitions = 0;
    TreeSet<TopicPartition> topicPartitions =
        new TreeSet( new KafkaUtils.TopicPartitionComparator());
    topicPartitions.addAll(cluster.topicPartitions.get(topic));

    for (TopicPartition topicPartition : topicPartitions) {
      writer.print("<tr>");

      double bytesInMax =
          cluster.getMaxBytesIn(topicPartition) / 1024.0 / 1024.0;
      double bytesOutMax =
          cluster.getMaxBytesOut(topicPartition) / 1024.0 / 1024.0;

      if (isZero(bytesInMax) && isZero(bytesOutMax)) {
        zeroTrafficPartitions++;
        continue;
      }

      String partitionHtml = String.format("<td>%d</td>", topicPartition.partition());
      String bytesInHtml = String.format("<td>%.2f</td>", bytesInMax);
      String bytesOutHtml = String.format("<td>%.2f</td>", bytesOutMax);
      writer.print(partitionHtml + bytesInHtml + bytesOutHtml );
      writer.print("</tr>");
    }
    writer.print("<tr> <td colspan=\"8\">" + zeroTrafficPartitions
        + " zero traffic partitions </td> </tr>");
    writer.print("</tbody> </table>");
  }

  private boolean isZero(double val) {
    return Math.abs(val - 0.0) < 0.00001;
  }
}
