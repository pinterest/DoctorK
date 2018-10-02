package com.pinterest.doctorkafka.servlet;


import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.errors.ClusterInfoError;

import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class UnderReplicatedPartitionsServlet extends HttpServlet {

  private static final Logger LOG = LogManager.getLogger(UnderReplicatedPartitionsServlet.class);
  private static final Gson gson = new Gson();
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String queryString = req.getQueryString();
    Map<String, String> params = DoctorKafkaServletUtil.parseQueryString(queryString);
    String clusterName = params.get("cluster");

    resp.setStatus(HttpStatus.OK_200);
    PrintWriter writer = resp.getWriter();

    String contentType = req.getHeader("content-type");
    if (contentType != null && contentType == "application/json") {
	resp.setContentType("application/json");
        renderJSON(clusterName,writer);
    } else {
      resp.setContentType("text/html");
    renderHTML(clusterName, writer);
    }
  }

  private void renderJSON(String clusterName, PrintWriter writer) {

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


  private void renderHTML(String clusterName, PrintWriter writer) {
    DoctorKafkaServletUtil.printHeader(writer);

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
    DoctorKafkaServletUtil.printFooter(writer);
  }
}
