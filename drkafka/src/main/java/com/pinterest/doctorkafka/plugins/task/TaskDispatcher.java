package com.pinterest.doctorkafka.plugins.task;

/**
 * TaskDispatcher is a instance that dispatches {@link Task}s that were received from plugins (e.g. {@link com.pinterest.doctorkafka.plugins.operator.Operator Operators}
 * , {@link TaskHandler Actions})
 * to actions that have subscribed to the task.
 */
public interface TaskDispatcher {

  /**
   * dispatches an Task to corresponding actions
   * @param task
   * @throws Exception
   */
  void dispatch(Task task) throws Exception;

  /**
   * subscribes the action to an task
   * @param taskName the name of the task the taskHandler is subscribing to
   * @param action
   * @throws Exception
   */
  void subscribe(String taskName, TaskHandler action) throws Exception;

  /**
   * start dispatching tasks
   */

  void start();

  /**
   * stop dispatching tasks
   */

  void stop();

}
