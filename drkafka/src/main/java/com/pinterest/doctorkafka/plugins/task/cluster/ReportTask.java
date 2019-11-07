package com.pinterest.doctorkafka.plugins.task.cluster;

import com.pinterest.doctorkafka.plugins.task.TaskUtils;

/**
 * This task is a helper for logging tasks
 */
public class ReportTask extends GenericTask {
  public ReportTask(String taskName, String subject, String message){
    super.setName(taskName);
    super.setAttribute(TaskUtils.TASK_SUBJECT_KEY , subject);
    super.setAttribute(TaskUtils.TASK_MESSAGE_KEY, message);
  }
}
