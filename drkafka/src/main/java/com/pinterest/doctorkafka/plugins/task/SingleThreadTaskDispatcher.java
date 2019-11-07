package com.pinterest.doctorkafka.plugins.task;

import com.pinterest.doctorkafka.plugins.operator.Operator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * SingleThreadTaskDispatcher is a task handler that implements a naive dispatcher of {@link Task}. It runs as
 * a pub-sub instance which delivers actions FIFO using a queue within a single thread. It also implements
 * the {@link TaskEmitter} interface such that {@link Operator Operators} can emit Tasks by calling the emit function.
 */
public class SingleThreadTaskDispatcher implements TaskEmitter, TaskDispatcher, Runnable {
  private static final Logger LOG = LogManager.getLogger(SingleThreadTaskDispatcher.class);
  private ExecutorService executorService = Executors.newSingleThreadExecutor();
  private volatile boolean stopped = false;
  private Map<String, Set<TaskHandler>> subscriptionMap= new ConcurrentHashMap<>();
  private BlockingQueue<Task> queue = new LinkedBlockingQueue<>();

  @Override
  public void subscribe(String taskName, TaskHandler action) {
    subscriptionMap.computeIfAbsent(taskName, name -> new HashSet<>()).add(action);
    LOG.info("Action {} has subscribed to task {}", action.getClass(), taskName);
  }

  @Override
  public void dispatch(Task task) {
    if(task != null){
      queue.add(task);
    }
  }

  protected void dispatchAll(Collection<Task> tasks) {
    if (tasks != null) {
      queue.addAll(tasks);
    }
  }

  @Override
  public void emit(Task task) {
    dispatch(task);
  }

  /**
   * Start the task handler
   */
  public void start(){
    executorService.submit(this);
  }

  public void stop(){
    stopped = true;
    executorService.shutdown();
  }

  @Override
  public void run() {
    try {
      while (!stopped){
        Task task = queue.take();
        String taskName = task.getName();
        for (TaskHandler action : subscriptionMap.get(taskName)){
          try {
            Collection<Task> newtasks = action.on(task);
            dispatchAll(newtasks);
          } catch (Exception e){
            LOG.error("Failed to execute action {}", action.getClass() , e);
          }
        }
      }
    } catch (InterruptedException e){
      LOG.info("taskHandler has been interrupted, terminating...");
    }
  }
}
