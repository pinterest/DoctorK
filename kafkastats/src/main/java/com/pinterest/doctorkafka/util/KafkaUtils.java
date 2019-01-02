package com.pinterest.doctorkafka.util;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String GROUP_ID = "group.id";
  public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
  public static final String KEY_DESERIALIZER = "key.deserializer";
  public static final String VALUE_DESERIALIZER = "value.deserializer";
  public static final String MAX_POLL_RECORDS = "max.poll.records";
  private static final int DEFAULT_MAX_POOL_RECORDS = 500;

  // Important: it is necessary to add any new top level Zookeeper path here
  // these paths are defined in kafka/core/src/main/scala/kafka/utils/ZkUtils.scala
  public static final String AdminPath = "/admin";
  public static final String ReassignPartitionsPath = AdminPath + "/reassign_partitions";
  public static final String PreferredReplicaLeaderElectionPath = AdminPath + "/preferred_replica_election";

  private static Map<String, KafkaConsumer<byte[], byte[]>> kafkaConsumers = new HashMap<>();
  private static Map<String, ZkUtils> zkUtilsMap = new HashMap<>();

  public static ZkUtils getZkUtils(String zkUrl) {
    if (!zkUtilsMap.containsKey(zkUrl)) {
      Tuple2<ZkClient, ZkConnection> zkClientAndConnection =
          ZkUtils.createZkClientAndConnection(zkUrl, 30000, 3000000);

      ZkUtils zkUtils = new ZkUtils(zkClientAndConnection._1(), zkClientAndConnection._2(), true);
      zkUtilsMap.put(zkUrl, zkUtils);
    }
    return zkUtilsMap.get(zkUrl);
  }


  public static List<ACL> getZookeeperAcls(boolean isSecure) {
    List<ACL> acls = new java.util.ArrayList<>();
    if (isSecure) {
      acls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
      acls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
    } else {
      acls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }
    return acls;
  }


  public static String getBrokers(String zkUrl, SecurityProtocol securityProtocol) {
    return OperatorUtil.getBrokers(zkUrl, securityProtocol);
  }

  /**
   * Get the under replicated nodes from PartitionInfo
   */
  public static Set<Node> getNotInSyncBrokers(PartitionInfo partitionInfo) {
    if (partitionInfo.inSyncReplicas().length == partitionInfo.replicas().length) {
      return new HashSet<>();
    }
    Set<Node> nodes = new HashSet<>(Arrays.asList(partitionInfo.replicas()));
    for (Node node : partitionInfo.inSyncReplicas()) {
      nodes.remove(node);
    }
    return nodes;
  }


  public static KafkaConsumer<byte[], byte[]> getKafkaConsumer(String zkUrl,
                                               String keyDeserializer,
                                               String valueDeserializer,
                                               int maxPoolRecords,
                                               SecurityProtocol securityProtocol,
                                               Map<String, String> otherConsumerConfigs) {
    String key = zkUrl;
    if (!kafkaConsumers.containsKey(key)) {
      String brokers = getBrokers(zkUrl, securityProtocol);
      LOG.info("ZkUrl: {}, Brokers: {}", zkUrl, brokers);
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "doctorkafka");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
      props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPoolRecords);
      props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576 * 4);

      if (otherConsumerConfigs != null) {
        for (Map.Entry<String, String> entry : otherConsumerConfigs.entrySet()) {
          props.put(entry.getKey(), entry.getValue());
        }
      }
      kafkaConsumers.put(key, new KafkaConsumer<>(props));
    }
    return kafkaConsumers.get(key);
  }

  public static KafkaConsumer<?, ?> getKafkaConsumer(String zkUrl,
                                               String keyDeserializer, String valueDeserializer) {
    return getKafkaConsumer(zkUrl, keyDeserializer, valueDeserializer,
        DEFAULT_MAX_POOL_RECORDS, SecurityProtocol.PLAINTEXT, null);
  }


  public static KafkaConsumer<byte[], byte[]> getKafkaConsumer(String zkUrl,
      SecurityProtocol securityProtocol,
      Map<String, String> consumerConfigs) {
    return getKafkaConsumer(zkUrl,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        DEFAULT_MAX_POOL_RECORDS, securityProtocol, consumerConfigs);
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
