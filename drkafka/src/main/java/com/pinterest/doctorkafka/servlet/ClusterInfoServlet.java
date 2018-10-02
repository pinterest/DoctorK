package com.pinterest.doctorkafka.servlet;

import com.google.gson.Gson;
import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.errors.ClusterInfoError;

import kafka.cluster.Broker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ClusterInfoServlet extends HttpServlet {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaServletUtil.class);
  private static final Gson gson = new Gson();

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String queryString = req.getQueryString();
    if (queryString == null) {
      resp.setStatus(HttpStatus.BAD_REQUEST_400);
      return;
    }
    resp.setStatus(HttpStatus.OK_200);
    PrintWriter writer = resp.getWriter();
    String contentType = req.getHeader("content-type");
    if (contentType != null && contentType == "application/json") {
	resp.setContentType("application/json");
        renderJSON(queryString, writer);
    } else {
	resp.setContentType("text/html");
        renderHTML(queryString, writer);
    }
  }

  public void renderJSON(String queryString, PrintWriter writer) {
    Map<String, String> params = DoctorKafkaServletUtil.parseQueryString(queryString);
    String clusterName = params.get("name");
    KafkaClusterManager clusterManager = DoctorKafkaMain.doctorKafka.getClusterManager(clusterName);

    if (clusterManager == null) {
      ClusterInfoError error = new ClusterInfoError("Failed to find cluster manager for " + clusterName);
      writer.print(gson.toJson(error));
      return ;
    }
    writer.print(gson.toJson(clusterManager.toJson()));
  }

  public void renderHTML(String queryString, PrintWriter writer) {
    DoctorKafkaServletUtil.printHeader(writer);
    try {
      Map<String, String> params = DoctorKafkaServletUtil.parseQueryString(queryString);
      String clusterName = params.get("name");

      KafkaClusterManager clusterMananger;
      clusterMananger = DoctorKafkaMain.doctorKafka.getClusterManager(clusterName);
      if (clusterMananger == null) {
        writer.print("Failed to find cluster manager for " + clusterName);
        return;
      }

      writer.print("<div> <p><a href=\"/\">Home</a> > " + clusterName + " </p> </div>");
      writer.print("<div> <h4> Cluster : " + clusterName + "</h4> </div>");

      KafkaCluster cluster = clusterMananger.getCluster();

      double totalMbIn = cluster.getMaxBytesIn() / 1024.0 /1024.0;
      double totalMbOut = cluster.getMaxBytesOut() / 1024.0 / 1024.0;

      // print overloaded and under-utilized brokers
      writer.print(String.format("<div> <p> Total bytes in max mean : %.2f Mb/sec, "
              + " Total bytes out max mean: %.2f Mb/sec </p> </div>",
          totalMbIn, totalMbOut));

      List<Broker> noStatsBrokers = clusterMananger.getNoStatsBrokers();
      if (!noStatsBrokers.isEmpty()) {
        writer.print(
            "<div class=\"container\"> No stats brokers (" + noStatsBrokers.size() + ") : ");
        for (Broker broker : noStatsBrokers) {
          writer.print( "<p>" +  broker  + "</p> <br/>");
        }
        writer.print("</div>");
      } else {
        List<KafkaBroker> overloadedBrokers = cluster.getHighTrafficBrokers();
        List<KafkaBroker> underutilized = cluster.getLowTrafficBrokers();
        Collections.sort(overloadedBrokers);
        Collections.reverse(overloadedBrokers);
        Collections.sort(underutilized);

        writer.print(String.format("<div class=\"container\"> overloaded brokers (%d) : ",
            overloadedBrokers.size()));
        for (KafkaBroker broker : overloadedBrokers) {
          writer.print(broker.name() + ",");
        }
        writer.print("</div>");

        writer.print(String.format("<div class=\"container\"> under-utilized brokers (%d): ",
            underutilized.size()));
        for (KafkaBroker broker : underutilized) {
          writer.print(broker.name() + ",");
        }
        writer.print("</div>");
      }

      writer.print("<table class=\"table table_stripped text-left\">");
      writer.print("<thead> <tr>");
      String thStr = String.format(
          "<th>%s</th> <th>%s</th> <th>%s</th> <th>%s</th> <th>%s</th> <th>%s</th>",
          "BrokerId", "BrokerName", "MaxIn (Mb/s)", "MaxOut (Mb/s)", "#Partitions", "Last Update");
      writer.print(thStr + "</tr> </thead>");
      writer.print("<tbody>");

      TreeMap<Integer, KafkaBroker> treeMap = new TreeMap<>();
      cluster.brokers.entrySet().stream().forEach(e -> treeMap.put(e.getKey(), e.getValue()));

      long now = System.currentTimeMillis();
      for (Map.Entry<Integer, KafkaBroker> brokerEntry : treeMap.entrySet()) {
        writer.print("<tr>");
        KafkaBroker broker = brokerEntry.getValue();
        double maxMbInPerSec = broker.getMaxBytesIn() / 1024.0 / 1024.0;
        double maxMbOutPerSec = broker.getMaxBytesOut() / 1024.0 / 1024.0;
        double lastUpdateTime = (now - broker.lastStatsTimestamp()) / 1000.0;
        
        String lastUpateTimeHtml =
            lastUpdateTime < 600
            ? String.format("<td> %.2f seconds ago </td>", lastUpdateTime)
            : String.format("<td class=\"text-danger\"> %.2f seconds ago </td>", lastUpdateTime);

        int partitionCount = broker.getLatestStats().getNumLeaders()+broker.getLatestStats().getNumReplicas();
        String html = String.format(
            "<td>%d</td> <td> %s </td> <td> %.2f</td> <td>%.2f</td> <td>%d</td> %s",
            brokerEntry.getKey(),
            "<a href=\"/servlet/brokerstats?cluster=" + clusterName
                + "&brokerid=" + broker.id() + "\">" + broker.name() + "</a>",
            maxMbInPerSec, maxMbOutPerSec, partitionCount, lastUpateTimeHtml);

        writer.print(html);
        writer.print("</tr>");
      }
      writer.print("</tbody> </table>");

      printTopicPartitionInfo(cluster, writer);

    } catch (Exception e) {
      LOG.error("Unexpected error", e);
    }
    DoctorKafkaServletUtil.printFooter(writer);
  }

  private void printTopicPartitionInfo(KafkaCluster cluster, PrintWriter writer) {

    writer.print("<div> <h4> Topics </h4> </div>");
    writer.print("<div> <table class=\"table\"> <tbody>");
    int topicId = 1;
    TreeSet<String> topics = new TreeSet<>(cluster.topics);
    for (String topic : topics) {
      writer.print("<tr> <td> " + topicId + "</td> <td>");
      writer.print("<a href=\"/servlet/topicstats?cluster=" + cluster.name()
          + "&topic=" + topic + "\">" + topic + "</a> </td> </tr>");
      topicId++;
    }
    writer.print("</tbody> </table> </div>");
  }
}
