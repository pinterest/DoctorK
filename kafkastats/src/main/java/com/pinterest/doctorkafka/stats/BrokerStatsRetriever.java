package com.pinterest.doctorkafka.stats;

import com.pinterest.doctorkafka.AvroTopicPartition;
import com.pinterest.doctorkafka.BrokerError;
import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.ReplicaStat;
import com.pinterest.doctorkafka.util.OperatorUtil;
import com.pinterest.doctorkafka.util.KafkaUtils;

import kafka.common.TopicAndPartition;
import kafka.controller.ReassignedPartitionsContext;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;


public class BrokerStatsRetriever {

  private static final String PROC_NET_DEV = "/proc/net/dev";
  private static final Logger LOG = LogManager.getLogger(BrokerStatsRetriever.class);
  private static final String VERSION = "0.2.4.8";
  private static String KAFKA_LOG = "kafka.log";
  private static String KAFKA_SERVER = "kafka.server";
  private static String LOG_DIR = "log.dir";
  private static String LOG_DIRS = "log.dirs";
  private static String SECURITY_INTER_BROKER_PROTOCOL = "security.inter.broker.protocol";
  private static String ZOOKEEPER_CONNECT = "zookeeper.connect";

  private String zkUrl = null;
  private String kafkaConfigPath;
  private BrokerStats brokerStats = null;
  private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
  
  private NetworkStats currentNetworkStats;
  private String primaryNetworkInterfaceName;
  private boolean disableEc2metadata;

  public BrokerStatsRetriever(String kafkaConfigPath, String primaryNetworkInterfaceName, boolean disableEc2metadata) {
    this.kafkaConfigPath = kafkaConfigPath;
    this.primaryNetworkInterfaceName = primaryNetworkInterfaceName;
    this.disableEc2metadata = disableEc2metadata;
  }


  private Map<String, Future<KafkaMetricValue>> getTopicMetrics(MBeanServerConnection mbs,
                                                                Set<String> topics,
                                                                String metricTemplate,
                                                                String attributeName) {
    Map<String, Future<KafkaMetricValue>> futures = new HashMap<>();
    for (String topic : topics) {
      String bytesInMetric = String.format(metricTemplate, topic);
      LOG.info(bytesInMetric);
      Future<KafkaMetricValue> metricValueFuture =
          MetricsRetriever.getMetricValue(mbs, bytesInMetric, attributeName);
      futures.put(topic, metricValueFuture);
    }
    return futures;
  }


  private Map<String, Long> getTopicNetworkMetricFromFutures(
      Map<String, Future<KafkaMetricValue>> futures)
      throws ExecutionException, InterruptedException {
    Map<String, Long> metricValues = new HashMap<>();

    for (Map.Entry<String, Future<KafkaMetricValue>> entry : futures.entrySet()) {
      Future<KafkaMetricValue> future = entry.getValue();
      KafkaMetricValue metricValue = future.get();
      if (!metricValue.getException()) {
        metricValues.put(entry.getKey(), metricValue.toLong());
      } else {
        LOG.warn("Got exception for {}", entry.getKey(), metricValue.exception);
      }
    }
    return metricValues;
  }

  public static double getProcessCpuLoad(MBeanServerConnection mbs)
      throws MalformedObjectNameException, NullPointerException, InstanceNotFoundException,
             ReflectionException, IOException {
    ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
    AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});

    if (list.isEmpty()) {
      return 0.0;
    }

    Attribute att = (Attribute) list.get(0);
    Double value = (Double) att.getValue();

    // usually takes a couple of seconds before we get real values
    if (value == -1.0) {
      return 0.0;
    }
    // returns a percentage value with 1 decimal point precision
    return ((int) (value * 1000) / 10.0);
  }
  
  /**
   * Parse system network stats from /proc/net/dev and extract stats for each device
   * 
   * e.g. 
   * Inter-|   Receive                                                |  Transmit
   * face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
   * eth0: 228953186524426 101855936261    0    0    0     0          0         0 243091098637101 104189136919    0    0    0     0       0          0
   * lo: 2866497873928 6603763924    0    0    0     0          0         0 2866497873928 6603763924    0    0    0     0       0          0
   * @return
   */
  public static Map<String, NetworkStats> getSystemNetworkStats() {
    Map<String, NetworkStats> deviceNetworkStatsMap = new HashMap<>();
    try {
      List<String> lines = Files.readAllLines(new File(PROC_NET_DEV).toPath());
      // ignore first two lines since they are header
      for (int i = 2; i < lines.size(); i++) {
        String line = lines.get(i);
        String[] splits = line.split("\\s+");
        String deviceName = splits[1].replace(":", "");
        
        NetworkStats deviceStats = new NetworkStats();
        deviceStats.setTimestamp(System.currentTimeMillis());
        
        deviceStats.setRxBytes(Long.parseLong(splits[2]));
        deviceStats.setRxPackets(Long.parseLong(splits[3]));
        
        deviceStats.setTxBytes(Long.parseLong(splits[10]));
        deviceStats.setTxPackets(Long.parseLong(splits[11]));
        
        deviceNetworkStatsMap.put(deviceName, deviceStats);
      }
    } catch (IOException e) {
      LOG.error("Failed to fetch system network stats", e);
    }
    return deviceNetworkStatsMap;
  }

  /**
   *  Kafka jmx metrics only provides per topic network inbound/outbound traffic statis. We need
   *  to compute per replica network traffic based on it. The key observation is that follower
   *  replicas should only have inbound traffic, and leader replicas have both inbound and
   *  outbound traffic. Also note that kafka only keeps track of BytesInPerSec and BytesOutPerSec
   *  metrics for *leader* partitions. See https://github.com/apache/kafka/blob/trunk/core/src/\
   *   main/scala/kafka/server/ReplicaManager.scala#L546 for details.
   *  Specifically, we can find the code path as follows;
   *
   *    partition.appendRecordsToLeader(records, isFromClient, requiredAcks)
   *     ...
   *    BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesInRate.mark(...)
   *    BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(records.sizeInBytes)
   *    BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).messagesInRate.mark(...)
   *    BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)
   *
   */
  private void computeTopicPartitionReplicaNetworkTraffic(
      List<ReplicaStat> replicaStats,
      Set<String> topics,
      Map<String, Long> topicsBytesIn1Min, Map<String, Long> topicsBytesOut1Min,
      Map<String, Long> topicsBytesIn5Min, Map<String, Long> topicsBytesOut5Min,
      Map<String, Long> topicsBytesIn15Min, Map<String, Long> topicsBytesOut15Min) {

    Map<String, List<ReplicaStat>> replicaStatsMap = new HashMap<>();
    Map<String, List<ReplicaStat>> leaderReplicaStatsMap = new HashMap<>();

    for (ReplicaStat replicaStat : replicaStats) {
      String topic = replicaStat.getTopic();
      replicaStatsMap.putIfAbsent(topic, new ArrayList<>());
      replicaStatsMap.get(topic).add(replicaStat);
      if (replicaStat.getIsLeader()) {
        leaderReplicaStatsMap.putIfAbsent(topic, new ArrayList<>());
        leaderReplicaStatsMap.get(topic).add(replicaStat);
      }
    }

    for (String topic : topics) {
      // Get the list of topic partitions
      if (!topicsBytesIn1Min.containsKey(topic) || !leaderReplicaStatsMap.containsKey(topic)) {
        LOG.info("{} does not have ByteInPerSec info", topic);
        continue;
      }
      List<ReplicaStat> leaderReplicaStats = leaderReplicaStatsMap.get(topic);

      long topicBytesIn1Min = topicsBytesIn1Min.get(topic);
      long topicBytesIn5Min = topicsBytesIn5Min.get(topic);
      long topicBytesIn15Min = topicsBytesIn15Min.get(topic);

      long leaderReplicasTotalBytes =
          leaderReplicaStats.stream().map(stat -> stat.getLogSizeInBytes()).reduce(0L, Long::sum);
      for (ReplicaStat stat : leaderReplicaStats) {
        double ratio = (double) stat.getLogSizeInBytes() / (double) leaderReplicasTotalBytes;
        long in1Min = Double.valueOf(topicBytesIn1Min * ratio).longValue();
        long in5Min = Double.valueOf(topicBytesIn5Min * ratio).longValue();
        long in15Min = Double.valueOf(topicBytesIn15Min * ratio).longValue();
        stat.setBytesIn1MinMeanRate(in1Min);
        stat.setBytesIn5MinMeanRate(in5Min);
        stat.setBytesIn15MinMeanRate(in15Min);
      }
    }

    for (String topic : topics) {
      if (!topicsBytesOut1Min.containsKey(topic) || topicsBytesOut1Min.get(topic) <= 0 ||
          !leaderReplicaStatsMap.containsKey(topic)) {
        continue;
      }
      long topicBytesOut1Min = topicsBytesOut1Min.get(topic);
      long topicBytesOut5Min = topicsBytesOut5Min.get(topic);
      long topicBytesOut15Min = topicsBytesOut15Min.get(topic);

      List<ReplicaStat> leaderReplicaStats = leaderReplicaStatsMap.get(topic);
      long replicasTotalBytes =
          leaderReplicaStats.stream().map(stat -> stat.getLogSizeInBytes()).reduce(0L, Long::sum);
      for (ReplicaStat stat : leaderReplicaStats) {
        double ratio = (double) stat.getLogSizeInBytes() / (double) replicasTotalBytes;
        long out1Min = Double.valueOf(topicBytesOut1Min * ratio).longValue();
        long out5Min = Double.valueOf(topicBytesOut5Min * ratio).longValue();
        long out15Min = Double.valueOf(topicBytesOut15Min * ratio).longValue();
        stat.setBytesOut1MinMeanRate(out1Min);
        stat.setBytesOut5MinMeanRate(out5Min);
        stat.setBytesOut15MinMeanRate(out15Min);
      }
    }
  }

  private void computeTopicPartitionReplicaCpuUsage(double totalCpuUsage,
                                                    List<ReplicaStat> replicaStats) {
    long totalTraffic = replicaStats.stream()
        .map(stat -> stat.getBytesIn1MinMeanRate() + stat.getBytesOut1MinMeanRate())
        .reduce(0L, Long::sum);

    replicaStats.stream().forEach(stat -> {
      double ratio = (double) (stat.getBytesIn1MinMeanRate() + stat.getBytesOut1MinMeanRate())
          / (double) totalTraffic;
      double tpCpuUsage = totalCpuUsage * ratio;
      stat.setCpuUsage(tpCpuUsage);
    });
  }


  private Set<TopicPartition> leaderReplicas = null;
  private Set<TopicPartition> replicas = null;
  private Set<TopicPartition> inSyncReplicas = null;
  private Set<String> topicNames = null;
  private Set<TopicPartition> inReassignReplicas = null;

  /**
   * Get stats on leader replicas, in-reassignment replica etc. through kafka api
   */
  private void retrieveStatsThroughKafkaApi() {
    Properties props = new Properties();
    String bootstrapBrokers = OperatorUtil.getBrokers(this.zkUrl, this.securityProtocol);
    props.put(KafkaUtils.BOOTSTRAP_SERVERS, bootstrapBrokers);
    props.put("group.id", "brokerstats_local");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

    leaderReplicas = new HashSet<>();
    replicas = new HashSet<>();
    inSyncReplicas = new HashSet<>();
    topicNames = new HashSet<>();
    inReassignReplicas = new HashSet<>();

    try {
      Map<String, List<PartitionInfo>> partitionInfoMap = kafkaConsumer.listTopics();
      for (String topic : partitionInfoMap.keySet()) {
        List<PartitionInfo> partitionInfos = partitionInfoMap.get(topic);
        for (PartitionInfo partitionInfo : partitionInfos) {
          TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
          if (partitionInfo.leader().id() == brokerStats.getId()) {
            leaderReplicas.add(tp);
          }
          for (Node node : partitionInfo.replicas()) {
            if (node.id() == brokerStats.getId()) {
              replicas.add(tp);
              topicNames.add(partitionInfo.topic());
            }
          }
          for (Node node : partitionInfo.inSyncReplicas()) {
            if (node.id() == brokerStats.getId()) {
              inSyncReplicas.add(tp);
            }
          }
        }
      }

      brokerStats.setNumReplicas(replicas.size());
      brokerStats.setNumLeaders(leaderReplicas.size());
      brokerStats.setLeaderReplicas(new ArrayList<>());
      for (TopicPartition tp : leaderReplicas) {
        AvroTopicPartition avroTp = new AvroTopicPartition(tp.topic(), tp.partition());
        brokerStats.getLeaderReplicas().add(avroTp);
      }

      brokerStats.setFollowerReplicas(new ArrayList<>());
      for (TopicPartition tp : replicas) {
        if (!leaderReplicas.contains(tp)) {
          AvroTopicPartition avroTp = new AvroTopicPartition(tp.topic(), tp.partition());
          brokerStats.getFollowerReplicas().add(avroTp);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to get replica info", e);
    } finally {
      kafkaConsumer.close();
    }

    // get the on-going partition reassignment information
    ZkUtils zkUtils = OperatorUtil.getZkUtils(zkUrl);
    scala.collection.Map<TopicAndPartition, ReassignedPartitionsContext>
        scalaTpMap = zkUtils.getPartitionsBeingReassigned();

    Map<TopicAndPartition, ReassignedPartitionsContext> tpMap =
        scala.collection.JavaConverters.mapAsJavaMap(scalaTpMap);
    brokerStats.setInReassignmentReplicas(new ArrayList<>());
    for (Map.Entry<TopicAndPartition, ReassignedPartitionsContext> entry : tpMap.entrySet()) {
      TopicPartition tp = new TopicPartition(entry.getKey().topic(), entry.getKey().partition());
      if (replicas.contains(tp)) {
        AvroTopicPartition avroTp = new AvroTopicPartition(tp.topic(), tp.partition());
        ReassignedPartitionsContext context = entry.getValue();
        brokerStats.getInReassignmentReplicas().add(avroTp);
        inReassignReplicas.add(tp);
      }
    }
  }


  private void retrieveNetworkStats(MBeanServerConnection mbs, Set<String> topics)
      throws ExecutionException, InterruptedException {
    Map<String, Future<KafkaMetricValue>> bytesIn1MinFutures = getTopicMetrics(mbs, topics,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=%s", "OneMinuteRate");
    Map<String, Future<KafkaMetricValue>> bytesOut1MinFutures = getTopicMetrics(mbs, topics,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=%s", "OneMinuteRate");

    Map<String, Future<KafkaMetricValue>> bytesIn5MinFutures = getTopicMetrics(mbs, topics,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=%s", "FiveMinuteRate");
    Map<String, Future<KafkaMetricValue>> bytesOut5MinFutures = getTopicMetrics(mbs, topics,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=%s", "FiveMinuteRate");

    Map<String, Future<KafkaMetricValue>> bytesIn15MinFutures = getTopicMetrics(mbs, topics,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=%s", "FifteenMinuteRate");
    Map<String, Future<KafkaMetricValue>> bytesOut15MinFutures = getTopicMetrics(mbs, topics,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=%s", "FifteenMinuteRate");

    Map<String, Long> bytesIn1MinRate = getTopicNetworkMetricFromFutures(bytesIn1MinFutures);
    Map<String, Long> bytesOut1MinRate = getTopicNetworkMetricFromFutures(bytesOut1MinFutures);
    Map<String, Long> bytesIn5MinRate = getTopicNetworkMetricFromFutures(bytesIn5MinFutures);
    Map<String, Long> bytesOut5MinRate = getTopicNetworkMetricFromFutures(bytesOut5MinFutures);
    Map<String, Long> bytesIn15MinRate = getTopicNetworkMetricFromFutures(bytesIn15MinFutures);
    Map<String, Long> bytesOut15MinRate = getTopicNetworkMetricFromFutures(bytesOut15MinFutures);

    brokerStats.setTopicsBytesIn1MinRate(bytesIn1MinRate);
    brokerStats.setTopicsBytesOut1MinRate(bytesOut1MinRate);
    brokerStats.setTopicsBytesIn5MinRate(bytesIn5MinRate);
    brokerStats.setTopicsBytesOut5MinRate(bytesOut5MinRate);
    brokerStats.setTopicsBytesIn15MinRate(bytesIn15MinRate);
    brokerStats.setTopicsBytesOut15MinRate(bytesOut15MinRate);

    // set the sum of network in/out traffic in bytes
    long inOneMinuteRateSum = bytesIn1MinRate.values().stream().reduce(0L, (a, b) -> a + b);
    long outOneMinuteRateSum = bytesOut1MinRate.values().stream().reduce(0L, (a, b) -> a + b);

    long in5MinRateSum = bytesIn5MinRate.values().stream().reduce(0L, (a, b) -> a + b);
    long out5MinRateSum = bytesOut5MinRate.values().stream().reduce(0L, (a, b) -> a + b);

    long in15MinRateSum = bytesIn15MinRate.values().stream().reduce(0L, (a, b) -> a + b);
    long out15MinRateSum = bytesOut15MinRate.values().stream().reduce(0L, (a, b) -> a + b);

    brokerStats.setLeadersBytesIn1MinRate(inOneMinuteRateSum);
    brokerStats.setLeadersBytesOut1MinRate(outOneMinuteRateSum);

    brokerStats.setLeadersBytesIn5MinRate(in5MinRateSum);
    brokerStats.setLeadersBytesOut5MinRate(out5MinRateSum);

    brokerStats.setLeadersBytesIn15MinRate(in15MinRateSum);
    brokerStats.setLeadersBytesOut15MinRate(out15MinRateSum);
  }


  /**
   *  set information on availability zone, instance type, ami-id, hostname etc.
   */
  private void setBrokerInstanceInfo() {
    BufferedReader input = null;
    brokerStats.setName(OperatorUtil.getHostname());

    // out quick if we don't want to use ec2metadata command.
    if (disableEc2metadata) {
      return;
    }
    
    try {
      Process process = Runtime.getRuntime().exec("ec2metadata");
      input = new BufferedReader(new InputStreamReader(process.getInputStream()));

      String outputLine;
      while ((outputLine = input.readLine()) != null) {
        String[] elements = outputLine.split(":");
        if (elements.length != 2) {
          continue;
        }
        if (elements[0].equals("availability-zone")) {
          brokerStats.setAvailabilityZone(elements[1].trim());
        } else if (elements[0].equals("instance-type")) {
          brokerStats.setInstanceType(elements[1].trim());
        } else if (elements[0].equals("ami-id")) {
          brokerStats.setAmiId(elements[1].trim());
        } else if (elements[0].equals("hostname")) { // This only works if the hostname is explicitly set in AWS
          brokerStats.setName(elements[1].trim());
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to get ec2 metadata", e);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          LOG.error("Failed to close bufferReader", e);
        }
      }
    }
  }

  /**
   *  set the broker id info for BrokerStats. First we try to find broker.id from
   *  kafka server.properties. If that is not found, we will check meta.properites under
   *  kafka log directory to get the broker id information.
   */
  private void setBrokerIdInfo(BrokerStats brokerStats, Properties kafkaProperties)
      throws IOException {
    String brokerIdStr = kafkaProperties.getProperty("broker.id");
    if (brokerIdStr == null) {
      // load from $kafka_log_dir/meta.properties
      String logDirsStr = kafkaProperties.getProperty("log.dirs").split(",")[0];
      kafkaProperties.load(new FileInputStream(logDirsStr + "/meta.properties"));
      brokerIdStr = kafkaProperties.getProperty("broker.id");
    }

    int brokerId = Integer.parseInt(brokerIdStr);
    brokerStats.setId(brokerId);
  }


  /**
   *  This method is to fill up the information for kafka version, brokerId, etc.
   */
  private void setBrokerConfiguration() {
    BufferedReader input = null;
    try {
      String outputLine;
      // getRuntime: Returns the runtime object associated with the current Java application.
      // exec: Executes the specified string command in a separate process.
      Process process = Runtime.getRuntime().exec("ps -few");
      input = new BufferedReader(new InputStreamReader(process.getInputStream()));


      while ((outputLine = input.readLine()) != null) {
        if (outputLine.contains("kafka.Kafka")) {
          int cpIndex = outputLine.indexOf("-cp");
          outputLine = outputLine.substring(cpIndex);
          String[] args = outputLine.replace('\t', ' ').split(" ");

          String classPathsStr = args[1];
          int clientJarBeginIndex = classPathsStr.indexOf("kafka-clients");
          int clientJarEndIndex = classPathsStr.indexOf(':', clientJarBeginIndex);
          String clientJar = classPathsStr.substring(clientJarBeginIndex, clientJarEndIndex);
          String[] substrs = clientJar.split("-");
          String version = substrs[2].replace(".jar", "");
          brokerStats.setKafkaVersion(version);
        }
      }

      Properties properties = new Properties();
      try {
        properties.load(new FileInputStream(kafkaConfigPath));
      } catch (IOException e) {
        LOG.error("Failed to load configuration file {}", kafkaConfigPath, e);
        brokerStats.setHasFailure(true);
        brokerStats.setFailureReason(BrokerError.KafkaServerProperitiesFailure);
        return;
      }

      setBrokerIdInfo(brokerStats, properties);
      zkUrl = properties.getProperty(ZOOKEEPER_CONNECT);
      brokerStats.setZkUrl(zkUrl);

      String securityProtocolStr = properties.containsKey(SECURITY_INTER_BROKER_PROTOCOL) ? properties.getProperty(SECURITY_INTER_BROKER_PROTOCOL)
	                                                                                  : "PLAINTEXT";
      securityProtocol = Enum.valueOf(SecurityProtocol.class, securityProtocolStr);

      // log.dirs specifies the directories in which the log data is kept.
      // If not set, the value in log.dir is used
      String logDirsStr = properties.containsKey(LOG_DIRS) ? properties.getProperty(LOG_DIRS)
                                                           : properties.getProperty(LOG_DIR);
      brokerStats.setLogFilesPath(logDirsStr);
      long freeSpaceInBytes = 0;
      long totalSpaceInBytes = 0;

      for (String logDir : logDirsStr.split(",")) {
        File tmpDir = new File(logDir);
        if (!tmpDir.exists()) {
          brokerStats.setHasFailure(true);
          brokerStats.setFailureReason(BrokerError.DiskFailure);
          return;
        } else {
          freeSpaceInBytes += tmpDir.getFreeSpace();
          totalSpaceInBytes += tmpDir.getTotalSpace();
        }
      }

      brokerStats.setFreeDiskSpaceInBytes(freeSpaceInBytes);
      brokerStats.setTotalDiskSpaceInBytes(totalSpaceInBytes);
    } catch (Exception e) {
      LOG.error("Failed to get broker configuration", e);
      brokerStats.setHasFailure(true);
      brokerStats.setFailureReason(BrokerError.UnknownError);
    } finally {
      try {
        if (input != null) {
          input.close();
        }
      } catch (Exception e) {
        LOG.error("Failed to close input stream", e);
      }
    }
  }

  private Set<ObjectName> getMetricObjectNames(MBeanServerConnection mbs) {
    Set<ObjectName> objectNames = null;
    try {
      objectNames = mbs.queryNames(null, null);
    } catch (IOException e) {
      LOG.error("Failed to get object names", e);
    }
    return objectNames;
  }


  /**
   *  The following is the steps for getting broker metrics:
   *     1. get all object names
   *     2. get topic partitions on the broker, and the leader brokers
   *     3. get BytesInPerSec metric for all topics
   *     4. get BytesOutPerSec metric for all topics
   *
   */
  public BrokerStats retrieveBrokerStats(String hostname, String jmxPort) throws Exception {
    // reset the broker stats object
    brokerStats = new BrokerStats();

    brokerStats.setHasFailure(false);
    long currentTime = System.currentTimeMillis();
    brokerStats.setTimestamp(currentTime);
    brokerStats.setStatsVersion(VERSION);

    // set information on availability zone, instance type, ami-id, hostname etc.
    setBrokerInstanceInfo();
    // set broker id, log dir, default retention hours etc.
    setBrokerConfiguration();

    if (brokerStats.getHasFailure()) {
      // return if we have encountered a failure.
      return brokerStats;
    }

    MBeanServerConnection mbs = OperatorUtil.getMBeanServerConnection(hostname, jmxPort);
    if (mbs == null) {
      brokerStats.setHasFailure(true);
      brokerStats.setFailureReason(BrokerError.JmxConnectionFailure);
      return brokerStats;
    }

    Set<ObjectName> objectNames = getMetricObjectNames(mbs);
    if (objectNames == null) {
      brokerStats.setHasFailure(true);
      brokerStats.setFailureReason(BrokerError.JmxQueryFailure);
      return brokerStats;
    }

    // get the reassignment partition info etc., and initialize fields like
    // leaderReplicas, replicas, inSyncReplicas, topicNames
    retrieveStatsThroughKafkaApi();

    // get bytesIn/bytesOut per second metrics
    retrieveNetworkStats(mbs, topicNames);

    // retrieve the stats of each replica
    List<Future<ReplicaStat>> replicaStatsFutures = new ArrayList();
    for (TopicPartition topicPartition : leaderReplicas) {
      boolean isLeader = leaderReplicas.contains(topicPartition);
      boolean inReassignment = inReassignReplicas.contains(topicPartition);
      Future<ReplicaStat> future = MetricsRetriever.getTopicPartitionReplicaStats(
          mbs, topicPartition, isLeader, inReassignment);
      replicaStatsFutures.add(future);
    }

    brokerStats.setLeaderReplicaStats(new ArrayList<>());
    for (Future<ReplicaStat> future : replicaStatsFutures) {
      ReplicaStat replicaStat = future.get();
      brokerStats.getLeaderReplicaStats().add(replicaStat);
    }

    // Kafka brokers only report network traffic at the topic level. If one leader replica
    // is involved in partition reassignment, the network traffic for that topic will be skewed.
    // Currently there is no way for us to distinguish the traffic among replicas.
    // Because of this, we are marking all leader replicas of the same topic as "InReassignment",
    // and have DoctorKafka service ignore these stats while computing the MaxIn/MaxOut stats
    // for those topic partitions.
    Set<String> topicsInReassigment = new HashSet<>();
    brokerStats.getLeaderReplicaStats().stream().filter(rs -> rs.getInReassignment())
        .forEach(replicaStat -> topicsInReassigment.add(replicaStat.getTopic()));
    for (ReplicaStat replicaStat : brokerStats.getLeaderReplicaStats()) {
      if (topicsInReassigment.contains(replicaStat.getTopic())) {
        replicaStat.setInReassignment(true);
      }
    }

    computeTopicPartitionReplicaNetworkTraffic(
        brokerStats.getLeaderReplicaStats(), topicNames,
        brokerStats.getTopicsBytesIn1MinRate(), brokerStats.getTopicsBytesOut1MinRate(),
        brokerStats.getTopicsBytesIn5MinRate(), brokerStats.getTopicsBytesOut5MinRate(),
        brokerStats.getTopicsBytesIn15MinRate(), brokerStats.getTopicsBytesOut15MinRate());

    double cpuLoad = getProcessCpuLoad(mbs);
    brokerStats.setCpuUsage(cpuLoad);
    computeTopicPartitionReplicaCpuUsage(cpuLoad, brokerStats.getLeaderReplicaStats());
    
    brokerStats.setHasFailure(false);
    return brokerStats;
  }
  
  private void computeNetworkStats(NetworkStats networkStats, BrokerStats brokerStats) {
    if(this.currentNetworkStats == null) {
      this.currentNetworkStats = networkStats;
    } else {
      // take delta of timestamp
      long deltaT = networkStats.getTimestamp() - currentNetworkStats.getTimestamp();
      // convert to seconds
      deltaT = deltaT / 1000;

      if (deltaT > 0) {
        long rxRate = networkStats.getRxBytes() - currentNetworkStats.getRxBytes();
        long txRate = networkStats.getTxBytes() - currentNetworkStats.getTxBytes();
        // per second average rate during the poll window
        brokerStats.setSysBytesIn1MinRate(rxRate / deltaT);
        brokerStats.setSysBytesOut1MinRate(txRate / deltaT);
      }
      // update the current for next poll
      currentNetworkStats = networkStats;
    }
  }

  public static class NetworkStats {
    
    private long timestamp;
    private long rxBytes;
    private long rxPackets;
    private long txBytes;
    private long txPackets;
    
    public NetworkStats() {
    }

    public NetworkStats(long rxBytes, long rxPackets, long txBytes,
        long txPackets) {
      this.rxBytes = rxBytes;
      this.rxPackets = rxPackets;
      this.txBytes = txBytes;
      this.txPackets = txPackets;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
      return timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    /**
     * @return the rxBytes
     */
    public long getRxBytes() {
      return rxBytes;
    }

    /**
     * @param rxBytes the rxBytes to set
     */
    public void setRxBytes(long rxBytes) {
      this.rxBytes = rxBytes;
    }

    /**
     * @return the rxPackets
     */
    public long getRxPackets() {
      return rxPackets;
    }

    /**
     * @param rxPackets the rxPackets to set
     */
    public void setRxPackets(long rxPackets) {
      this.rxPackets = rxPackets;
    }

    /**
     * @return the txBytes
     */
    public long getTxBytes() {
      return txBytes;
    }

    /**
     * @param txBytes the txBytes to set
     */
    public void setTxBytes(long txBytes) {
      this.txBytes = txBytes;
    }

    /**
     * @return the txPackets
     */
    public long getTxPackets() {
      return txPackets;
    }

    /**
     * @param txPackets the txPackets to set
     */
    public void setTxPackets(long txPackets) {
      this.txPackets = txPackets;
    }
    
  }
}
