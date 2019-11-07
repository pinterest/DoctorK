package com.pinterest.doctorkafka.plugins.task;

import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.plugins.Plugin;
import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.operator.Operator;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

/**
 *
 * Actions are plugins that perform single operations based on certain {@link Task}s. On initialization,
 * each action will subscribe to the cluster's {@link TaskDispatcher} to the tasks listed in the configuration.
 *
 * <pre>
 *                                +------------------+
 *                     Subscribe  |                  |
 *            Action +----------->|  taskDispatcher |
 *                                |                  |
 *                                +------------------+
 * </pre>
 *
 * Once the evaluation loops in {@link KafkaClusterManager} starts and {@link Operator}s emit tasks,
 * the taskDispatcher will collect the tasks and dispatch tasks to Actions that have
 * subscribed to the corresponding task.
 *
 * <pre>
 *               +-----------------+
 *               |                 |
 *               | Operator/Action |
 *               |                 |
 *               +-----------------+
 *                        +
 *                        |              execute +---------+
 *                      task           +------->| Action  |
 *                        |             |        +---------+
 *                        v             |
 *               +-----------------+    |
 *               |                 |    |execute +---------+
 *               | taskDispatcher |+-->+------->| Action  |
 *               |                 |    |        +---------+
 *               +-----------------+    |
 *                                      |
 *                                      |execute +---------+
 *                                      +------->| Action  |
 *                                               +---------+
 *
 * </pre>
 */
public abstract class TaskHandler implements Plugin {
  private static final Logger LOG = LogManager.getLogger(TaskHandler.class);
  private static final String CONFIG_SUBSCRIBED_TASKS_KEY = "subscribed_tasks";
  private static final String CONFIG_DRY_RUN_KEY = "dryrun";

  private String[] subscribedtasks;
  private boolean dryrun = true;

  /**
   * This method is called when an task that this Action has subscribed to has been fired.
   * @param task task that the action has subscribed to
   * @return A set of tasks that will be fired after this action has been executed.
   * @throws Exception
   */
  public final Collection<Task> on(Task task) throws Exception{
    if(dryrun) {
      LOG.info("Dry run: Action {} triggered by task {}", this.getClass(), task.getName());
      return null;
    } else {
      LOG.debug("Executing Action {} triggered by task {}", this.getClass(), task.getName());
      return execute(task);
    }
  }

  /**
   * This method is called to execute the main logic of an Action
   * @param task task that the action has subscribed to
   * @return A set of tasks that will be fired after this action has been executed.
   * @throws Exception
   */
  public abstract Collection<Task> execute(Task task) throws Exception;

  @Override
  public final void initialize(ImmutableConfiguration config) throws PluginConfigurationException {
    if (!config.containsKey(CONFIG_SUBSCRIBED_TASKS_KEY)){
      throw new PluginConfigurationException("Missing task subscriptions for action " + this.getClass());
    }
    subscribedtasks = config.getString(CONFIG_SUBSCRIBED_TASKS_KEY).split(",");
    dryrun = config.getBoolean(CONFIG_DRY_RUN_KEY, dryrun);
    configure(config);
  }

  public final String[] getSubscribedTasks(){
    return subscribedtasks;
  }
}
