package com.pinterest.doctorkafka.tools;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * This program is to fetch a specific metrics from jmx
 *  Usage:  com.pinterest.kafka.metricsfetecher  --host kafkahost --port 9999
 *             --metrics $metric_name
 */
public class MetricsFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsFetcher.class);
  private static final String BROKER_NAME = "host";
  private static final String JMX_PORT = "port";
  private static final String METRICS_NAME = "metric";

  private static final Options options = new Options();

  private static CommandLine parseCommandLine(String[] args) {

    Option host = new Option(BROKER_NAME, true, "kafka broker");
    Option jmxPort = new Option(JMX_PORT, true, "kafka jmx port number");
    jmxPort.setArgName("kafka jmx port number");

    Option metric = new Option(METRICS_NAME, true, "jmx metric name");

    options.addOption(jmxPort).addOption(host).addOption(metric);

    if (args.length < 4) {
      printUsageAndExit();
    }
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException | NumberFormatException e) {
      printUsageAndExit();
    }
    return cmd;
  }

  private static void printUsageAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("KafkaMetricsCollector", options);
    System.exit(1);
  }


  private static void fetchKafkaMetrics(String host, String jmxPort, String metric)
      throws Exception {

    Map<String, String[]> env = new HashMap<>();
    JMXServiceURL address = new JMXServiceURL(
        "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":" + jmxPort + "/jmxrmi");
    JMXConnector connector = JMXConnectorFactory.connect(address, env);
    MBeanServerConnection mbs = connector.getMBeanServerConnection();


    ObjectName name = ObjectName.getInstance(metric);
    MBeanInfo beanInfo = mbs.getMBeanInfo(name);
    for (MBeanAttributeInfo attributeInfo : beanInfo.getAttributes()) {
      Object obj = mbs.getAttribute(name, attributeInfo.getName());
      System.out.println(" attributeName = " + attributeInfo.getName() + " " + obj.toString());
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);

    String host = commandLine.getOptionValue(BROKER_NAME);
    String jmxPort = commandLine.getOptionValue(JMX_PORT);
    String metric = commandLine.getOptionValue(METRICS_NAME);
    fetchKafkaMetrics(host, jmxPort, metric);
  }
}
