package com.pinterest.doctorkafka.servlet;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.ReplicaStat;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.errors.ClusterInfoError;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Date;
import java.lang.Integer;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class DoctorKafkaBrokerStatsServlet extends DoctorKafkaServlet {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaBrokerStatsServlet.class);
  private static final Gson gson = (new GsonBuilder()).serializeSpecialFloatingPointValues().create();

  public BrokerStats getLatestStats(String clusterName, int brokerId)
    throws ClusterInfoError {

    try {
      KafkaClusterManager clusterMananger =
	DoctorKafkaMain.doctorKafka.getClusterManager(clusterName);
      if (clusterMananger == null) {
	throw new ClusterInfoError("Failed to find cluster manager for {}", clusterName);
      }
      KafkaCluster cluster = clusterMananger.getCluster();
      KafkaBroker broker = cluster.brokers.get(brokerId);
      BrokerStats latestStats = broker.getLatestStats();
      if (latestStats == null) {
	throw new ClusterInfoError("Failed to find Broker {} for {} ", Integer.toString(brokerId), clusterName);
      }
      return latestStats;
    } catch (Exception e) {
      LOG.error("Unexpected exception : ", e);
      throw new ClusterInfoError("Unexpected exception: {} ", e.toString());
    }
  }
  
  @Override
  public void renderJSON(PrintWriter writer, Map<String, String> params) {
    try {
      int brokerId = Integer.parseInt(params.get("brokerid"));
      String clusterName = params.get("cluster");
      BrokerStats latestStats = getLatestStats(clusterName, brokerId);
      writer.print(gson.toJson(latestStats));
    } catch (Exception e) {
      LOG.error("Unable to find cluster : {}", e.toString());
      writer.print(gson.toJson(e));
      return;
    }
  }

  @Override
  public void renderHTML(PrintWriter writer, Map<String, String> params) {
    int brokerId = Integer.parseInt(params.get("brokerid"));
    String clusterName = params.get("cluster");
    printHeader(writer);

    writer.print("<div> <p><a href=\"/\">Home</a> > "
		 + "<a href=\"/servlet/clusterinfo?name=" + clusterName + "\"> " + clusterName
		 + "</a> > broker " + brokerId + "</p> </div>");

    writer.print("<table class=\"table table-hover\"> ");
    writer.print("<th class=\"active\"> Timestamp </th> ");
    writer.print("<th class=\"active\"> Stats </th>");
    writer.print("<tbody>");

    try {
      BrokerStats latestStats = getLatestStats(clusterName, brokerId);
      generateBrokerStatsHtml(writer, latestStats);
      writer.print("</tbody></table>");
      writer.print("</td> </tr>");
      writer.print("</tbody> </table>");
    } catch (Exception e) {
      LOG.error("Unexpected exception : ", e);
      e.printStackTrace(writer);
    }
    printFooter(writer);
  }

  private void generateBrokerStatsHtml(PrintWriter writer, BrokerStats stats) {
    writer.print("<tr> <td> " + new Date(stats.getTimestamp()) + "</td>");
    writer.print("<td>");
    writer.print("<table class=\"table\"><tbody>");
    printHtmlTableRow(writer, "BrokerId", stats.getId());
    printHtmlTableRow(writer, "Name", stats.getName());
    printHtmlTableRow(writer, "HasFailure", stats.getHasFailure());
    printHtmlTableRow(writer, "KafkaVersion", stats.getKafkaVersion());
    printHtmlTableRow(writer, "KafkaStatsVersion", stats.getStatsVersion());
    printHtmlTableRow(writer, "LeadersIn1MinRate",
        NumberFormat.getNumberInstance(Locale.US).format(stats.getLeadersBytesIn1MinRate()));
    printHtmlTableRow(writer, "BytesInOneMinuteRate", NumberFormat.
        getNumberInstance(Locale.US).format(stats.getLeadersBytesIn1MinRate()));
    printHtmlTableRow(writer, "NetworkOutboundOneMinuteRate", NumberFormat
        .getNumberInstance(Locale.US).format(stats.getLeadersBytesOut1MinRate()));
    printHtmlTableRow(writer, "NumTopicPartitionReplicas",
        NumberFormat.getNumberInstance(Locale.US).format(stats.getNumReplicas()));
    printHtmlTableRow(writer, "NumLeaderPartitions", stats.getNumLeaders());

    Map<TopicPartition, ReplicaStat> replicaStats =
        new TreeMap(new KafkaUtils.TopicPartitionComparator());
    stats.getLeaderReplicaStats().stream()
        .forEach(
            rs -> replicaStats.put(new TopicPartition(rs.getTopic(), rs.getPartition()), rs));
    for (Map.Entry<TopicPartition, ReplicaStat> entry : replicaStats.entrySet()) {
      printHtmlTableRow(writer, entry.getKey(), entry.getValue());
    }
  }

  private void printHtmlTableRow(PrintWriter writer, Object col1, Object col2) {
    writer.print("<tr><td>" + col1 + "</td> <td>" + col2 + "</td> </tr>");
  }

}
