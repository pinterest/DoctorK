package com.pinterest.doctorkafka.servlet;


import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.DoctorKafkaMain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DoctorKafkaInfoServlet extends HttpServlet {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaInfoServlet.class);

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    resp.setContentType("text/html");
    resp.setStatus(HttpStatus.OK_200);

    PrintWriter writer = resp.getWriter();
    try {
      double jvmUpTimeInSeconds = ManagementFactory.getRuntimeMXBean().getUptime() / 1000.0;
      String version = DoctorKafkaServletUtil.getVersion();
      writer.print("<div>");
      writer.print("<p> Version: " + version + ", Uptime: " + jvmUpTimeInSeconds + " seconds </p>");
      writer.print("</div>");

      Collection<KafkaClusterManager> clusterManagers = DoctorKafkaMain.doctorKafka.getClusterManagers();
      writer.print("<div> ");
      writer.print("<table class=\"table table-responsive\"> ");
      writer.print("<th> ClusterName </th> <th> Size </th> <th> Under-replicated Partitions</th>");
      writer.print("<th> Maintenance Mode </th>");
      writer.print("<tbody>");

      Map<String, String>  clustersHtml = new TreeMap<>();
      for (KafkaClusterManager clusterManager : clusterManagers) {
        String clusterName = clusterManager.getClusterName();
        String htmlStr;
        htmlStr = "<tr> <td> <a href=\"/servlet/clusterinfo?name=" + clusterName + "\">"
              + clusterName + "</a>"
              + " </td> <td> " + ((clusterManager.getCluster()!=null)? clusterManager.getClusterSize() : "no brokerstats")
              + " </td> <td> <a href=\"/servlet/urp?cluster=" + clusterName + "\">"
              + clusterManager.getUnderReplicatedPartitions().size() + "</a> </td>"
              + "<td>" + clusterManager.isMaintenanceModeEnabled() + " </td> </tr>";

        clustersHtml.put(clusterName, htmlStr);
      }
      for (Map.Entry<String, String> entry : clustersHtml.entrySet()) {
        writer.print(entry.getValue());
      }
      writer.print("</tbody> </table>");
      writer.print("</div>");
    } catch (Exception e) {
      LOG.error("Exception in getting info", e);
      writer.println(e);
    }
  }
}
