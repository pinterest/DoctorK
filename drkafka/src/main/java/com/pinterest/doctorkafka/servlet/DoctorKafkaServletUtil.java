package com.pinterest.doctorkafka.servlet;

import static java.util.stream.Collectors.toList;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.common.TopicPartition;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DoctorKafkaServletUtil {

  public static void printHeader(PrintWriter writer) {
    writer.print("<html>");
    writer.print("<head>");
    writer.print("<meta charset=\"utf-8\" >");
    writer.print("<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">");
    writer.print("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    writer.print("<link rel=\"stylesheet\"");
    writer.print(" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\"");
    writer.print(" integrity=");
    writer.print("\"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u\" ");
    writer.print("crossorigin=\"anonymous\" >");

    writer.print("<link rel=\"stylesheet\" href=");
    writer.print("\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css\"");
    writer.print(" integrity= \"");
    writer.print("sha384-rHyoN1iRsVXV4nD0JutlnGaslCJuC7uwjduW9SVrLvRYooPp2bWYgmgJQIXwl/Sp\" ");
    writer.print("crossorigin=\"anonymous\">");
    writer.print("<title>KafkaOperator Latest Actions</title>");

    writer.print("<script src=\"");
    writer.print("https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js\" ");
    writer.print("integrity=\"");
    writer.print("sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa\" ");
    writer.print("crossorigin=\"anonymous\"></script>");
    writer.print("</head>");

    writer.print("<body>");

    writer.print("<div class=\"container\">");
    writer.print("<div class=\"page-header\">");
    writer.print("<h2>");
    writer.print("<img src=\"https://svn.apache.org/repos/asf/kafka/site/logos/originals"
        + "/png/ICON%20-%20Black%20on%20Transparent.png\" width=\"64px\"/>");
    writer.print("DoctorKafka</h2>");
    writer.print("</div>");
  }

  public static void printFooter(PrintWriter writer) {
    writer.print("</body>");
    writer.print("<html>");
  }


  public static Map<String, String> parseQueryString(String queryString) {
    Map<String, String> result = new HashMap<>();
    Arrays.stream(queryString.split("&")).map(s -> s.split("=")).collect(toList())
        .forEach(arr -> result.put(arr[0], arr[1]));
    return result;
  }

  public static Map<String, List<DescriptiveStatistics>> convertDescriptiveStatisticsMap(
      Map<TopicPartition, DescriptiveStatistics> tpsMap) {
    Map<String, List<DescriptiveStatistics>> result = new TreeMap<>();
    for (Map.Entry<TopicPartition, DescriptiveStatistics> entry : tpsMap.entrySet()) {
      String topicName = entry.getKey().topic();
      int partitionId = entry.getKey().partition();
      if (!result.containsKey(topicName)) {
        result.put(topicName, new ArrayList<>());
      }
      List<DescriptiveStatistics> partitionArray = result.get(topicName);
      while (partitionArray.size() < partitionId + 1) {
        partitionArray.add(null);
      }
      partitionArray.set(partitionId, entry.getValue());
    }
    return result;
  }
}
