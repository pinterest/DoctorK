package com.pinterest.doctork.plugins.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * SharedThreadPoolTaskDispatcher is a {@link TaskDispatcher} backed by a thread pool
 * shared across all DoctorK cluster managers. This allows concurrent actions to fire
 * without getting blocked by long-running action execution.
 *
 * The shared thread pool uses the default common ForkJoinPool for CompletableFutures
 *
 * The ActionTask class is a wrapper of actions. We enforce thread-safe in ActionTask by
 * synchronizing on the Action before execution.
 */
public class SharedThreadPoolTaskDispatcher implements TaskEmitter, TaskDispatcher, Runnable {

  private static final Logger LOG = LogManager.getLogger(SharedThreadPoolTaskDispatcher.class);

  // 2 threads in this pool, 1 for the `run` thread, 1 for callback and exception handling
  private final ExecutorService taskHandlerThreads = Executors.newFixedThreadPool(2);
  private Map<String, Set<TaskHandler>> subscriptionMap = new ConcurrentHashMap<>();
  private BlockingQueue<Task> queue = new LinkedBlockingQueue<>();

  private volatile boolean stopped = false;

  @Override
  public void subscribe(String taskName, TaskHandler action) {
    subscriptionMap.computeIfAbsent(taskName, name -> new HashSet<>()).add(action);
    LOG.info("Action {} has subscribed to task {}", action.getClass(), taskName);
  }

  @Override
  public void dispatch(Task task) {
    if (task != null) {
      queue.add(task);
    }
  }

  protected void dispatchAll(Collection<Task> tasks) {
    if (tasks != null) {
      // Not using addAll here since BlockingQueue doesn't guarantee thread-safety of addAll
      for (Task task : tasks) {
        dispatch(task);
      }
    }
  }

  @Override
  public void emit(Task task) {
    dispatch(task);
  }

  /**
   * Start the task handler
   */
  public void start() {
    taskHandlerThreads.submit(this);
  }

  public void stop() {
    stopped = true;
    taskHandlerThreads.shutdown();
  }

  @Override
  public void run() {
    try {
      while (!stopped) {
        Task task = queue.take();
        String taskName = task.getName();
        for (TaskHandler action : subscriptionMap.get(taskName)) {
          ActionTask actionTask = new ActionTask(action, task);
          CompletableFuture
              .supplyAsync(actionTask::runTask)
              .thenAcceptAsync(
                  this::dispatchAll,
                  taskHandlerThreads) // use local threads instead of ForkJoinPool
              .whenCompleteAsync((v, e) -> {
                if (e != null) {
                  LOG.error("Error when running action task: ", e);
                }
              }, taskHandlerThreads);
        }
      }
    } catch (InterruptedException e) {
      LOG.info("TaskDispatcher has been interrupted, terminating...");
    }
  }

  private class ActionTask {

    private TaskHandler handler;
    private Task task;

    ActionTask(TaskHandler handler, Task task) {
      this.handler = handler;
      this.task = task;
    }

    public Collection<Task> runTask() {

      /* synchronizing on the Action instance for thread-safety
         (i.e. an action under a cluster should not be executed concurrently) */
      synchronized (handler) {
        try {
          return handler.on(task);
        } catch (Exception e) {
          throw new CompletionException("Failed to execute handler " + handler.getClass(), e);
        }
      }
    }

  }

}
