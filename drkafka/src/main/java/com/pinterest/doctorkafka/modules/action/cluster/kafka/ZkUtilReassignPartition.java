package com.pinterest.doctorkafka.modules.action.cluster.kafka;

import com.pinterest.doctorkafka.modules.action.errors.ExistingReassignmentException;
import com.pinterest.doctorkafka.util.KafkaUtils;

import kafka.utils.ZkUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.ACL;

import java.util.List;

public class ZkUtilReassignPartition implements ReassignPartition {
  private static final Logger LOG = LogManager.getLogger(ZkUtilReassignPartition.class);

  @Override
  public void reassign(String zkUrl, String jsonReassignment) throws Exception{
    ZkUtils zkUtils = KafkaUtils.getZkUtils(zkUrl);
    if (zkUtils.pathExists(KafkaUtils.ReassignPartitionsPath)) {
      throw new ExistingReassignmentException(zkUrl + " has ongoing reassignment.");
    } else {
      List<ACL> acls = KafkaUtils.getZookeeperAcls(false);
      zkUtils.createPersistentPath(KafkaUtils.ReassignPartitionsPath, jsonReassignment, acls);
    }
  }
}
