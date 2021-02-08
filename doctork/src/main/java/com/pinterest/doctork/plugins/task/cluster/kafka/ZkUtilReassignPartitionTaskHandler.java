package com.pinterest.doctork.plugins.task.cluster.kafka;

import com.pinterest.doctork.plugins.task.Task;
import com.pinterest.doctork.plugins.task.TaskHandler;
import com.pinterest.doctork.plugins.task.TaskUtils;
import com.pinterest.doctork.plugins.task.cluster.NotificationTask;
import com.pinterest.doctork.plugins.task.cluster.ReportTask;
import com.pinterest.doctork.util.KafkaUtils;

import kafka.utils.ZkUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.ACL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This action uses ZkUtil to reassign partitions based on a JSON reassignment
 *
 * <pre>
 * Input Task Format:
 * {
 *   zkurl: str,
 *   reassignment_json: str (JSON format),
 *   cluster_name: str (Default: "n/a")
 * }
 *
 * Output Tasks Format:
 *
 * Task: notify_reassignment:
 * triggered when reassignment is kicked off
 * {
 *   title: str,
 *   message: str
 * }
 *
 * Task: report_operation:
 * triggered when reassignment is kicked off
 * {
 *   subject: str,
 *   message: str
 * }
 * </pre>
 */
public class ZkUtilReassignPartitionTaskHandler extends TaskHandler {
  
  private static final Logger LOG = LogManager.getLogger(ZkUtilReassignPartitionTaskHandler.class);

  private static final String TASK_REPORT_OPERATION_NAME = "report_operation";
  private static final String TASK_NOTIFY_REASSIGNMENT_NAME = "notify_reassignment";

  private static final String TASK_REASSIGNMENT_JSON_KEY = "reassignment_json";

  private final static String DEFAULT_TASK_SUBJECT = "n/a";

  @Override
  public Collection<Task> execute(Task task) throws Exception {
    if(task.containsAttribute(TaskUtils.TASK_ZKURL_KEY) && task.containsAttribute(
        TASK_REASSIGNMENT_JSON_KEY)) {
      String clusterName = task.containsAttribute(TaskUtils.TASK_CLUSTER_NAME_KEY) ?
                           (String) task.getAttribute(TaskUtils.TASK_CLUSTER_NAME_KEY) :
                           DEFAULT_TASK_SUBJECT;
      String zkUrl = (String) task.getAttribute(TaskUtils.TASK_ZKURL_KEY);
      String jsonReassignment= (String) task.getAttribute(TASK_REASSIGNMENT_JSON_KEY);
      return reassign(zkUrl, clusterName, jsonReassignment);
    }
    return null;
  }

  protected Collection<Task> reassign(String zkUrl, String clusterName, String jsonReassignment) throws Exception {
    ZkUtils zkUtils = KafkaUtils.getZkUtils(zkUrl);
    Collection<Task> tasks = null;
    if (zkUtils.pathExists(KafkaUtils.ReassignPartitionsPath)) {
      LOG.warn("Existing reassignment on zookeeper: {}", zkUrl);
    } else {
      List<ACL> acls = KafkaUtils.getZookeeperAcls(false);
      zkUtils.createPersistentPath(KafkaUtils.ReassignPartitionsPath, jsonReassignment, acls);
      tasks = createReassignedTasks(clusterName, jsonReassignment);
    }
    return tasks;
  }

  protected Collection<Task> createReassignedTasks(String clusterName, String jsonReassignment){
    Collection<Task> tasks = new ArrayList<>();
    tasks.add(new ReportTask(TASK_REPORT_OPERATION_NAME, clusterName, "Reassigning partitions: " + jsonReassignment));

    String title = clusterName + " partition reassignment";
    String message = "Assignment json: \n\n" + jsonReassignment;
    tasks.add(new NotificationTask(TASK_NOTIFY_REASSIGNMENT_NAME, title , message));
    return tasks;
  }
}
