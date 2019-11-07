package com.pinterest.doctorkafka.plugins.operator;

import com.pinterest.doctorkafka.plugins.Plugin;
import com.pinterest.doctorkafka.plugins.context.state.State;
import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;
import com.pinterest.doctorkafka.plugins.task.Task;
import com.pinterest.doctorkafka.plugins.task.TaskEmitter;

import org.apache.commons.configuration2.ImmutableConfiguration;


/**
 * An operator takes the {@link State} of a service and builds a plan of operations to remediate/alert on the service.
 * Operators emit {@link Task} to trigger these actions.
 * <pre>
 +------------+               +-----------+    +-----------+
 |            |    +-----+    |           |    |           |
 |  Monitor   |+-->|State|+-->| Operator1 |+-->| Operator2 |
 |            |    +-----+    |           |    |           |
 +------------+               +-----------+    +-----------+
                                    +                +
                                    | emit           | emit
                                    v                v
                                 +-----+          +-----+
                                 |task |          |task |
                                 +-----+          +-----+
                                    +                +
                                    |                |
                                    +----------------+
                                            |
                                            v
                                    Actions/task Handling
 </pre>
 */
public abstract class Operator implements Plugin {
  private TaskEmitter taskEmitter;

  public void setTaskEmitter(TaskEmitter taskEmitter){
    this.taskEmitter = taskEmitter;
  }

  /**
   * Sends an task to trigger actions
   * @param task task that provides context to the subscribed action(s)
   * @throws Exception
   */
  public void emit(Task task) throws Exception {
    taskEmitter.emit(task);
  }

  /**
   * @param state The state derived from {@link com.pinterest.doctorkafka.plugins.monitor.Monitor Monitors}
   * @return false if later operations should not be executed, true otherwise.
   * @throws Exception
   */
  public abstract boolean operate(State state) throws Exception;

  @Override
  public final void initialize(ImmutableConfiguration config) throws PluginConfigurationException {
    configure(config);
  }
}
