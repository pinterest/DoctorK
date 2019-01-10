package com.pinterest.doctorkafka.servlet;


import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.errors.ClusterInfoError;
import com.pinterest.doctorkafka.servlet.DoctorKafkaServletUtil;

import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class UnderReplicatedPartitionsServlet extends DoctorKafkaServletUtil {

  private static final Logger LOG = LogManager.getLogger(UnderReplicatedPartitionsServlet.class);
  private static final Gson gson = new Gson();

  @Override
  public void renderJSON(PrintWriter writer, Map<String, String> params) {
    String clusterName = params.get("cluster");
    KafkaClusterManager clusterMananger =
        DoctorKafkaMain.doctorKafka.getClusterManager(clusterName);

    if (clusterMananger == null) {
      ClusterInfoError error = new ClusterInfoError("Failed to find cluster manager for {}", clusterName);
      writer.print(gson.toJson(error));
      return;
    }

    List<PartitionInfo> urps = clusterMananger.getUnderReplicatedPartitions();
    JsonArray json = new JsonArray();

    for (PartitionInfo partitionInfo : urps) {
      json.add(gson.toJsonTree(partitionInfo));
    }
    writer.print(json);
  }

  @Override
  public void renderHTML(PrintWriter writer, Map<String, String> params) {
    String clusterName = params.get("cluster");
    printHeader(writer);

    KafkaClusterManager clusterMananger =
        DoctorKafkaMain.doctorKafka.getClusterManager(clusterName);

    if (clusterMananger == null) {
      writer.print("Failed to find cluster manager for " + clusterName);
      return;
    }

    writer.print("<div> <p><a href=\"/\">Home</a> > "
        + clusterName + " under replicated partitions </p> </div>");

    List<PartitionInfo> urps = clusterMananger.getUnderReplicatedPartitions();
    writer.print("<table class=\"table\"> <tbody>");

    for (PartitionInfo partitionInfo : urps) {
      writer.print("<tr> <td>");
      writer.print(partitionInfo);
      writer.print("</td> </tr>");
    }
    writer.print("</tbody> </table>");
    printFooter(writer);
  }
}
