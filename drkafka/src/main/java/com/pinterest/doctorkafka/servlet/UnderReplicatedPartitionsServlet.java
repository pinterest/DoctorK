package com.pinterest.doctorkafka.servlet;


import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.KafkaClusterManager;

import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;

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

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String queryString = req.getQueryString();
    Map<String, String> params = DoctorKafkaServletUtil.parseQueryString(queryString);
    String clusterName = params.get("cluster");

    resp.setStatus(HttpStatus.OK_200);
    PrintWriter writer = resp.getWriter();

    DoctorKafkaServletUtil.printHeader(writer);

    KafkaClusterManager clusterMananger = DoctorKafkaMain.operator.getClusterManager(clusterName);

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