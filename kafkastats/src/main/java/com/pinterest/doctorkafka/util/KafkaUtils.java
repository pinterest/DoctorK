package com.pinterest.doctorkafka.util;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaUtils {

  private static final Logger LOG = LogManager.getLogger(KafkaUtils.class);
  public static final String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String GROUP_ID = "group.id";
  public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
  public static final String KEY_DESERIALIZER = "key.deserializer";
  public static final String VALUE_DESERIALIZER = "value.deserializer";
  public static final String SESSION_TIMEOUT_MS = "session.timeout.ms";
  public static final String MAX_POLL_RECORDS = "max.poll.records";

  private static final int DEFAULT_MAX_POOL_RECORDS = 500;

  // Important: it is necessary to add any new top level Zookeeper path here
  // these paths are defined in kafka/core/src/main/scala/kafka/utils/ZkUtils.scala
  public static final String AdminPath = "/admin";
  public static final String ReassignPartitionsPath = AdminPath + "/reassign_partitions";
  public static final String
      PreferredReplicaLeaderElectionPath = AdminPath + "/preferred_replica_election";

  private static Map<String, KafkaConsumer> kafkaConsumers = new HashMap();
  private static Map<String, ZkUtils> zkUtilsMap = new HashMap();

  public static ZkUtils getZkUtils(String zkUrl) {
    if (!zkUtilsMap.containsKey(zkUrl)) {
      Tuple2<ZkClient, ZkConnection> zkClientAndConnection =
          ZkUtils.createZkClientAndConnection(zkUrl, 300, 3000000);

      ZkUtils zkUtils = new ZkUtils(zkClientAndConnection._1(), zkClientAndConnection._2(), true);
      zkUtilsMap.put(zkUrl, zkUtils);
    }
    return zkUtilsMap.get(zkUrl);
  }


  public static List<ACL> getZookeeperAcls(boolean isSecure) {
    List<ACL> acls = new java.util.ArrayList();
    if (isSecure) {
      acls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
      acls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
    } else {
      acls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }
    return acls;
  }


  public static String getBrokers(String zkUrl) {
    return OperatorUtil.getBrokers(zkUrl);
  }

  /**
   * Get the under replicated nodes from PartitionInfo
   */
  public static Set<Node> getNotInSyncBrokers(PartitionInfo partitionInfo) {
    if (partitionInfo.inSyncReplicas().length == partitionInfo.replicas().length) {
      return new HashSet<>();
    }
    Set<Node> nodes = new HashSet(Arrays.asList(partitionInfo.replicas()));
    for (Node node : partitionInfo.inSyncReplicas()) {
      nodes.remove(node);
    }
    return nodes;
  }


  public static KafkaConsumer getKafkaConsumer(String zkUrl,
                                               String keyDeserializer,
                                               String valueDeserializer,
                                               int maxPoolRecords) {
    String key = zkUrl;
    if (!kafkaConsumers.containsKey(key)) {
      String brokers = getBrokers(zkUrl);
      LOG.info("ZkUrl: {}, Brokers: {}", zkUrl, brokers);
      Properties props = new Properties();
      props.put(BOOTSTRAP_SERVERS, brokers);
      props.put(ENABLE_AUTO_COMMIT, "false");
      props.put(GROUP_ID, "kafka_operator");
      props.put(KEY_DESERIALIZER, keyDeserializer);
      props.put(VALUE_DESERIALIZER, valueDeserializer);
      props.put(MAX_POLL_RECORDS, maxPoolRecords);

      props.put("max.partition.fetch.bytes", 1048576 * 4);
      kafkaConsumers.put(key, new KafkaConsumer(props));
    }
    return kafkaConsumers.get(key);
  }


  public static KafkaConsumer getKafkaConsumer(String zkUrl,
                                               String keyDeserializer, String valueDeserializer) {
    return getKafkaConsumer(zkUrl, keyDeserializer, valueDeserializer, DEFAULT_MAX_POOL_RECORDS);
  }


  public static KafkaConsumer getKafkaConsumer(String zkUrl) {
    return getKafkaConsumer(zkUrl,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer", DEFAULT_MAX_POOL_RECORDS);
  }


  public static void closeConsumer(String zkUrl) {
    if (kafkaConsumers.containsKey(zkUrl)) {
      kafkaConsumers.get(zkUrl).close();
      kafkaConsumers.remove(zkUrl);
    }
  }


  public static class TopicPartitionComparator implements Comparator<TopicPartition> {

    @Override
    public int compare(TopicPartition x, TopicPartition y) {
      int result = x.topic().compareTo(y.topic());
      if (result == 0) {
        result = x.partition() - y.partition();
      }
      return result;
    }
  }

  public static class NodeComparator implements Comparator<Node> {

    @Override
    public int compare(Node a, Node b) {
      int result = a.host().compareTo(b.host());
      if (result == 0) {
        result = a.port() - b.port();
      }
      return result;
    }
  }
}
