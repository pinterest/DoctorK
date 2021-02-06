package com.pinterest.doctork.plugins.task.cluster;

import com.pinterest.doctork.plugins.task.TaskUtils;

/**
 * This task is a helper for notification actions
 */
public class NotificationTask extends GenericTask {
  
  public NotificationTask(String taskName, String title, String message){
    super.setName(taskName);
    super.setAttribute(TaskUtils.TASK_TITLE_KEY, title);
    super.setAttribute(TaskUtils.TASK_MESSAGE_KEY, message);
  }
  
}
