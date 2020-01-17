package com.pinterest.doctorkafka.util;


import com.google.common.net.HostAndPort;
import com.pinterest.doctorkafka.BrokerStats;

import org.apache.commons.lang3.tuple.MutablePair;

import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Tuple2;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class OperatorUtil {

  private static final Logger LOG = LogManager.getLogger(OperatorUtil.class);
  public static final String HostName = getHostname();
  private static final DecoderFactory avroDecoderFactory = DecoderFactory.get();
  private static final int API_REQUEST_TIMEOUT_MS = 10000;
  // Reuse the reader to improve performance
  private static final SpecificDatumReader<BrokerStats> brokerStatsReader = new SpecificDatumReader<>(BrokerStats.getClassSchema());

  public static String getHostname() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
      int firstDotPos = hostName.indexOf('.');
      if (firstDotPos > 0) {
        hostName = hostName.substring(0, firstDotPos);
      }
    } catch (Exception e) {
      // fall back to env var.
      hostName = System.getenv("HOSTNAME");
    }
    return hostName;
  }

  /**
   * Get an MBeanServerConnection object. Return null if there is any failure.
   */
  public static MBeanServerConnection getMBeanServerConnection(String host, String jmxPort) {
    MBeanServerConnection mbs = null;
    try {
      Map<String, String[]> env = new HashMap<>();
      JMXServiceURL address = new JMXServiceURL(
          "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":" + jmxPort + "/jmxrmi");
      JMXConnector connector = JMXConnectorFactory.connect(address, env);
      mbs = connector.getMBeanServerConnection();
    } catch (Exception e) {
      LOG.error("Failed to connect to MBeanServer {}:{}", HostName, jmxPort, e);
    }
    return mbs;
  }

  public static boolean pingKafkaBroker(String host, int port, int timeout) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), timeout);
      return true;
    } catch (UnknownHostException e) {
      LOG.warn("Ping failure, host: " + host, e);
      return false;
    } catch (IOException e) {
      LOG.warn("Ping failure IO, host: " + host, e);
      return false; // Either timeout or unreachable or failed DNS lookup.
    }
  }

  public static boolean canServeApiRequests(String host, int port) {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
    AdminClient adminClient = AdminClient.create(props);
    DescribeClusterResult result = adminClient.describeCluster();
    try {
      result.clusterId().get(API_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return true;
    } catch (Exception e) {
      LOG.warn("Metadata fetch from endpoint {} failed.", host + ":" + port, e);
      return false;
    }
  }

  private static Map<String, ZkUtils> zkUtilsMap = new HashMap();

  public static ZkUtils getZkUtils(String zkUrl) {
    if (!zkUtilsMap.containsKey(zkUrl)) {
      Tuple2<ZkClient, ZkConnection> zkClientAndConnection =
          ZkUtils.createZkClientAndConnection(zkUrl, 30000, 3000000);

      ZkUtils zkUtils = new ZkUtils(zkClientAndConnection._1(), zkClientAndConnection._2(), true);
      zkUtilsMap.put(zkUrl, zkUtils);
    }
    return zkUtilsMap.get(zkUrl);
  }

  public static String getBrokers(String zkUrl, SecurityProtocol securityProtocol) {
    ZkUtils zkUtils = getZkUtils(zkUrl);
    Seq<Broker> brokersSeq = zkUtils.getAllBrokersInCluster();
    Broker[] brokers = new Broker[brokersSeq.size()];
    brokersSeq.copyToArray(brokers);

    String brokersStr = Arrays.stream(brokers)
        .map(b -> b.brokerEndPoint(
            ListenerName.forSecurityProtocol(securityProtocol)).connectionString())
        .reduce(null, (a, b) -> (a == null) ? b : a + "," + b);
    return brokersStr;
  }

  public static Properties createKafkaProducerProperties(String zkUrl, SecurityProtocol securityProtocol) {
    String bootstrapBrokers = OperatorUtil.getBrokers(zkUrl, securityProtocol);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1638400);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 3554432);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaUtils.BYTE_ARRAY_SERIALIZER);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaUtils.BYTE_ARRAY_SERIALIZER);
    return props;
  }

  public static Properties createKafkaConsumerProperties(String zkUrl, String consumerGroupName,
      SecurityProtocol securityProtocol, Properties consumerConfigs) {
    String bootstrapBrokers = OperatorUtil.getBrokers(zkUrl, securityProtocol);
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaUtils.BYTE_ARRAY_DESERIALIZER);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaUtils.BYTE_ARRAY_DESERIALIZER);

    if (consumerConfigs != null) {
      for (Entry<Object, Object> entry : consumerConfigs.entrySet()) {
        props.put(entry.getKey(), entry.getValue());
      }
    }
    return props;
  }

  public static BrokerStats deserializeBrokerStats(ConsumerRecord<byte[], byte[]> record) {
    try {
      BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(record.value(), null);
      BrokerStats stats = new BrokerStats();
      brokerStatsReader.read(stats, binaryDecoder);
      return stats;
    } catch (Exception e) {
      LOG.debug("Fail to decode an message", e);
      return null;
    }
  }

  public static void startOstrichService(String serviceName, String tsdbHostPort, int ostrichPort) {
    final int TSDB_METRICS_PUSH_INTERVAL_IN_MILLISECONDS = 10 * 1000;
    OstrichAdminService ostrichService = new OstrichAdminService(ostrichPort);
    ostrichService.startAdminHttpService();
    if (tsdbHostPort != null) {
      LOG.info("Starting the OpenTsdb metrics pusher");
      try {
        HostAndPort pushHostPort = HostAndPort.fromString(tsdbHostPort);
        MetricsPusher metricsPusher = new MetricsPusher(
            pushHostPort.getHost(),
            pushHostPort.getPort(),
            new OpenTsdbMetricConverter(serviceName, HostName),
            TSDB_METRICS_PUSH_INTERVAL_IN_MILLISECONDS);
        metricsPusher.start();
        LOG.info("OpenTsdb metrics pusher started!");
      } catch (Throwable t) {
        // pusher fail is OK, do
        LOG.error("Exception when starting stats pusher: ", t);
      }
    }
  }

  public static MutablePair<Long, Long> getProcNetDevStats() throws Exception {
    ProcessBuilder ps = new ProcessBuilder("cat", "/proc/net/dev");
    Process pr = ps.start();
    pr.waitFor();

    BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
    String line;
    int counter = 0;
    long receivedBytes = 0;
    long outBytes = 0;

    while ((line = in.readLine()) != null) {
      System.out.println(counter + ": " + line);
      if (line.contains("eth0")) {
        String[] strs = line.split(" ");
        receivedBytes = Long.parseLong(strs[3]);
        outBytes = Long.parseLong(strs[41]);
        System.out.println(" inBytes = " + receivedBytes + "  outBytes = " + outBytes);
      }
      counter++;
    }
    in.close();

    MutablePair<Long, Long> result = new MutablePair<>(receivedBytes, outBytes);
    return result;
  }

  public static MutablePair<Double, Double> getSysNetworkTraffic(long samplingWindowInMs)
      throws Exception {
    MutablePair<Long, Long> startNumbers = getProcNetDevStats();
    Thread.sleep(samplingWindowInMs);
    MutablePair<Long, Long> endNumbers = getProcNetDevStats();

    double inRate = (endNumbers.getKey() - startNumbers.getKey()) * 1000.0 / samplingWindowInMs;
    double outRate =
        (endNumbers.getValue() - startNumbers.getValue()) * 1000.0 / samplingWindowInMs;
    MutablePair<Double, Double> result = new MutablePair<>(inRate, outRate);
    return result;
  }

  /**
   * Sort the map entries based on entry values in descending order
   */
  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
    return map.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (e1, e2) -> e1,
            LinkedHashMap::new
        ));
  }
}
