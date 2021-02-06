package com.pinterest.doctork.servlet;

import com.google.gson.Gson;
import com.pinterest.doctork.KafkaBroker;
import com.pinterest.doctork.DoctorKMain;
import com.pinterest.doctork.KafkaCluster;
import com.pinterest.doctork.KafkaClusterManager;
import com.pinterest.doctork.errors.ClusterInfoError;

import kafka.cluster.Broker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class ClusterInfoServlet extends DoctorKServlet {

  private static final Logger LOG = LogManager.getLogger(ClusterInfoServlet.class);
  private static final Gson gson = new Gson();

  @Override
  public void renderJSON(PrintWriter writer, Map<String, String> params) {
    String clusterName;
    try {
      clusterName = params.get("name");
    } catch (Exception e) {
      LOG.error("'name' parameter not found");
      return ;
    }

    try {
      KafkaClusterManager clusterManager = DoctorKMain.doctorK.getClusterManager(clusterName);
      if (clusterManager == null) {
        ClusterInfoError error = new ClusterInfoError("Failed to find cluster manager for " + clusterName);
        writer.print(gson.toJson(error));
        return ;
      }
      writer.print(gson.toJson(clusterManager.toJson()));
    } catch (Exception e) {
      LOG.error("Unexpected error: {}", e);
      throw(e);
    }
  }

  @Override
  public void renderHTML(PrintWriter writer, Map<String, String> params) {
    try {
      printHeader(writer);
      String clusterName = params.get("name");
      KafkaClusterManager clusterMananger;
      clusterMananger = DoctorKMain.doctorK.getClusterManager(clusterName);
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
          writer.print(broker.getName() + ",");
        }
        writer.print("</div>");

        writer.print(String.format("<div class=\"container\"> under-utilized brokers (%d): ",
            underutilized.size()));
        for (KafkaBroker broker : underutilized) {
          writer.print(broker.getName() + ",");
        }
        writer.print("</div>");
      }

      writer.print("<table class=\"table table_stripped text-left\">");
      writer.print("<thead> <tr>");
      String thStr = String.format(
          "<th>%s</th> <th>%s</th> <th>%s</th> <th>%s</th> <th>%s</th> <th>%s</th> <th>%s</th>",
          "BrokerId", "BrokerName", "MaxIn (Mb/s)", "MaxOut (Mb/s)", "#Partitions", "Last Update",
          "Decommissioned");
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
        double lastUpdateTime = (now - broker.getLastStatsTimestamp()) / 1000.0;
        
        String lastUpateTimeHtml =
            lastUpdateTime < 600
            ? String.format("<td> %.2f seconds ago </td>", lastUpdateTime)
            : String.format("<td class=\"text-danger\"> %.2f seconds ago </td>", lastUpdateTime);

        int partitionCount = broker.getLatestStats().getNumReplicas();
        String html = String.format(
            "<td>%d</td> <td> %s </td> <td> %.2f</td> <td>%.2f</td> <td>%d</td> %s <td> %s </td>",
            brokerEntry.getKey(),
            "<a href=\"/servlet/brokerstats?cluster=" + clusterName
                + "&brokerid=" + broker.getId() + "\">" + broker.getName() + "</a>",
            maxMbInPerSec, maxMbOutPerSec, partitionCount, lastUpateTimeHtml,
            broker.isDecommissioned());

        writer.print(html);
        writer.print("</tr>");
      }
      writer.print("</tbody> </table>");
      printTopicPartitionInfo(cluster, writer);
    } catch (Exception e) {
      LOG.error("Unexpected error", e);
    }
    printFooter(writer);
  }

  private void printTopicPartitionInfo(KafkaCluster cluster, PrintWriter writer) {
    writer.print("<div> <h4> Topics </h4> </div>");
    writer.print("<div> <table class=\"table\"> <tbody>");
    int topicId = 1;
    TreeSet<String> topics = new TreeSet<>(cluster.topicPartitions.keySet());
    for (String topic : topics) {
      writer.print("<tr> <td> " + topicId + "</td> <td>");
      writer.print("<a href=\"/servlet/topicstats?cluster=" + cluster.name()
          + "&topic=" + topic + "\">" + topic + "</a> </td> </tr>");
      topicId++;
    }
    writer.print("</tbody> </table> </div>");
  }
}
