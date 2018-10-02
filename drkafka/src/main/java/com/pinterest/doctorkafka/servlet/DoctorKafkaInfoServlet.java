package com.pinterest.doctorkafka.servlet;


import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.DoctorKafkaMain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

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
  private static final Gson gson = new Gson();

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    resp.setStatus(HttpStatus.OK_200);

    PrintWriter writer = resp.getWriter();
    String contentType = req.getHeader("content-type");
    if (contentType != null && contentType == "application/json") {
	resp.setContentType("application/json");
        renderJSON(writer);
    } else {
      resp.setContentType("text/html");
      renderHTML(writer);
    }
    
  }


  public void renderJSON(PrintWriter writer) {
    JsonObject json = new JsonObject();
    json.add("version", gson.toJsonTree(DoctorKafkaServletUtil.getVersion()));
    json.add("uptime", gson.toJsonTree(ManagementFactory.getRuntimeMXBean().getUptime() / 1000.0));
    JsonArray jsonClusters = new JsonArray();
    json.add("clusters", jsonClusters);

    List<KafkaClusterManager> clusterManagers = DoctorKafkaMain.doctorKafka.getClusterManagers();

    for (KafkaClusterManager clusterManager : clusterManagers) {
      JsonObject cluster = new JsonObject();
      cluster.add("clusterName", gson.toJsonTree(clusterManager.getClusterName()));
      if (clusterManager.getCluster() != null) {
	cluster.add("size", gson.toJsonTree(clusterManager.getClusterSize()));
	cluster.add("urps", gson.toJsonTree(clusterManager.getUnderReplicatedPartitions().size()));
      } else {
	cluster.add("size", gson.toJsonTree(0));
	cluster.add("urps", gson.toJsonTree(0));
      }
      jsonClusters.add(cluster);
    }

    writer.print(json);
    
  }
  

  private void renderHTML(PrintWriter writer) {
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

      Map<String, String> clustersHtml = new TreeMap<>();
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
