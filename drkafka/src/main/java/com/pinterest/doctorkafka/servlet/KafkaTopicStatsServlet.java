package com.pinterest.doctorkafka.servlet;


import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.util.KafkaUtils;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeSet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class KafkaTopicStatsServlet extends HttpServlet {

  private static final Logger LOG = LogManager.getLogger(KafkaTopicStatsServlet.class);

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    String queryString = req.getQueryString();
    Map<String, String> params = DoctorKafkaServletUtil.parseQueryString(queryString);
    String clusterName = params.get("cluster");
    String topic = params.get("topic");

    resp.setStatus(HttpStatus.OK_200);

    PrintWriter writer = resp.getWriter();
    try {
      DoctorKafkaServletUtil.printHeader(writer);
      writer.print("<div> <p><a href=\"/\">Home</a> > "
          + "<a href=\"/servlet/clusterinfo?name=" + clusterName + "\"> " + clusterName
          + "</a> > " + topic + "</p> </div>");

      // generate the HTML markups
      KafkaClusterManager clusterMananger =
          DoctorKafkaMain.doctorKafka.getClusterManager(clusterName);
      if (clusterMananger == null) {
        writer.print("Failed to find cluster manager for " + clusterName);
        return;
      }
      writer.print("<div> <h4> Cluster : " + clusterName + "</h4> </div>");
      KafkaCluster cluster = clusterMananger.getCluster();
      printTopicPartitionInfo(cluster, writer, topic);
      DoctorKafkaServletUtil.printFooter(writer);

    } catch (Exception e) {
      e.printStackTrace(writer);
    }
  }


  private void printTopicPartitionInfo(KafkaCluster cluster, PrintWriter writer, String topic) {

    writer.print("<p> <h5>" + topic + "</h5></p>");
    writer.print("<table class=\"table\">");
    writer.print("<thead> <tr> <th> Partition</th> ");
    writer.print("<th>In Max</th> ");
    writer.print("<th>Out max</th>");
    writer.print("</tr> </thead> <tbody>");

    int zeroTrafficPartitions = 0;
    TreeSet<TopicPartition> topicPartitions =
        new TreeSet( new KafkaUtils.TopicPartitionComparator());
    topicPartitions.addAll(cluster.topicPartitions.get(topic));

    for (TopicPartition topicPartition : topicPartitions) {
      writer.print("<tr>");

      double bytesInMax =
          ReplicaStatsManager.getMaxBytesIn(cluster.zkUrl, topicPartition) / 1024.0 / 1024.0;
      double bytesOutMax =
          ReplicaStatsManager.getMaxBytesOut(cluster.zkUrl, topicPartition) / 1024.0 / 1024.0;

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
        + " empty partitions </td> </tr>");
    writer.print("</tbody> </table>");
  }

  private boolean isZero(double val) {
    return Math.abs(val - 0.0) < 0.00001;
  }
}
