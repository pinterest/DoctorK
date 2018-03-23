package com.pinterest.doctorkafka.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.ostrich.stats.Stats;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *  DoctorKafka uses zookeeper to keep metadata
 *   zkurl/doctorkafka/clusters/${clustername}/alerts
 *                                            /actions
 */
public class ZookeeperClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);
  private static final String DOCTORKAFKA_PREFIX = "/doctorkafka";
  private static final int MAX_RETRIES = 5;
  private static final long RETRY_INTERVAL_MS = 100L;

  private String zookeeperConnection;
  private CuratorFramework curator = null;

  public ZookeeperClient(String zkUrl) {
    zookeeperConnection = zkUrl;
    LOG.info("Initialize curator ");
    curator = CuratorFrameworkFactory.newClient(zookeeperConnection, new RetryOneTime(3000));
    curator.start();
  }

  @Override
  public void close() {
    curator.close();
  }

  public CuratorFramework getCurator() {
    return curator;
  }

  private void waitBetweenRetries(int numRetries) {
    try {
      Thread.sleep(RETRY_INTERVAL_MS * numRetries);
    } catch (InterruptedException ex) {
      LOG.error("Interrupted in waiting", ex);
    }
  }

  public boolean createIfNotExists(String path) {
    int numRetries = 0;
    while (numRetries < MAX_RETRIES) {
      try {
        Stat stat = curator.checkExists().forPath(path);
        if (stat == null) {
          curator.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT)
              .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
              .forPath(path);
        }
        return true;
      } catch (Exception e) {
        LOG.error("Failed to create zk path {}", path, e);
        numRetries++;
        waitBetweenRetries(numRetries);
      }
    }
    return false;
  }

  public boolean setData(String path, String content) {
    boolean success = createIfNotExists(path);
    if (!success) {
      LOG.error("Failed to create zk path {}", path);
      return false;
    }

    int numRetries = 0;
    while (numRetries < MAX_RETRIES) {
      try {
        curator.setData().forPath(path, content.getBytes());
        return true;
      } catch (Exception e) {
        LOG.error("Failure in setData: {} : {}", path, content, e);
        numRetries++;
        waitBetweenRetries(numRetries);
      }
    }
    return false;
  }

  public void removeZkNode(String path) throws Exception {
    try {
      curator.delete().forPath(path);
    } catch (Exception e) {
      LOG.error("Failed to remove zk node {}", path);
      Stats.incr("merced.zookeeper.failure.remove_zknode");
      throw e;
    }
  }

  public List<String> getChildren(String path) throws Exception {
    List<String> children = curator.getChildren().forPath(path);
    return children;
  }


  private String getBrokerReplacementPath(String clusterName) {
    String path = DOCTORKAFKA_PREFIX + "/" + clusterName + "/actions/broker_replacement";
    return path;
  }


  public boolean recordBrokerTermination(String clusterName, String brokerName) {
    String path = getBrokerReplacementPath(clusterName);
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("timestamp", System.currentTimeMillis());
    jsonObject.addProperty("cluster", clusterName);
    jsonObject.addProperty("broker", brokerName);
    String content = jsonObject.toString();
    return setData(path, content);
  }

  public String getDataInString(String path) throws Exception {
    String retval;
    byte[] data = curator.getData().forPath(path);
    retval = new String(data);
    return retval;
  }

  public String getBrokerReplacementInfo(String cluster) throws Exception {
    String path = getBrokerReplacementPath(cluster);
    String jsonStr = getDataInString(path);
    return jsonStr;
  }

  public long getLastBrokerReplacementTime(String clusterName) throws Exception {
    String path = getBrokerReplacementPath(clusterName);
    Stat stat = curator.checkExists().forPath(path);
    long timestamp = -1;
    if (stat != null) {
      String jsonStr = getDataInString(path);
      JsonObject jsonObject = (JsonObject) (new JsonParser()).parse(jsonStr);
      timestamp = jsonObject.get("timestamp").getAsLong();
    }
    return timestamp;
  }

  public String getLastReplacedBroker(String cluster) throws Exception {
    String path = getBrokerReplacementPath(cluster);
    Stat stat = curator.checkExists().forPath(path);
    String broker = null;
    if (stat != null) {
      String jsonStr = getDataInString(path);
      JsonObject jsonObject = (JsonObject) (new JsonParser()).parse(jsonStr);
      broker = jsonObject.get("broker").toString();
    }
    return broker;
  }
}
