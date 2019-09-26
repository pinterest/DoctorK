package com.pinterest.doctorkafka.plugins.context.event;

import com.pinterest.doctorkafka.plugins.action.Action;

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
 * SharedThreadPoolEventHandler is an EventHandler backed by a thread pool
 * shared across all DoctorKafka cluster managers. This allows concurrent actions to fire
 * without getting blocked by long-running action execution.
 *
 * The shared thread pool uses the default common ForkJoinPool for CompletableFutures
 *
 * The ActionTask class is a wrapper of actions. We enforce thread-safe in ActionTask by
 * synchronizing on the Action before execution.
 */
public class SharedThreadPoolEventHandler implements EventEmitter, EventDispatcher, Runnable {

  private static final Logger LOG = LogManager.getLogger(SharedThreadPoolEventHandler.class);

  // 2 threads in this pool, 1 for the `run` thread, 1 for callback and exception handling
  private final ExecutorService eventHandlerThreads = Executors.newFixedThreadPool(2);
  private Map<String, Set<Action>> subscriptionMap = new ConcurrentHashMap<>();
  private BlockingQueue<Event> queue = new LinkedBlockingQueue<>();

  private volatile boolean stopped = false;

  @Override
  public void subscribe(String eventName, Action action) {
    subscriptionMap.computeIfAbsent(eventName, name -> new HashSet<>()).add(action);
    LOG.info("Action {} has subscribed to event {}", action.getClass(), eventName);
  }

  @Override
  public void dispatch(Event event) {
    if (event != null) {
      queue.add(event);
    }
  }

  protected void dispatchAll(Collection<Event> events) {
    if (events != null) {
      // Not using addAll here since BlockingQueue doesn't guarantee thread-safety of addAll
      for (Event event : events) {
        dispatch(event);
      }
    }
  }

  @Override
  public void emit(Event event) {
    dispatch(event);
  }

  /**
   * Start the event handler
   */
  public void start() {
    eventHandlerThreads.submit(this);
  }

  public void stop() {
    stopped = true;
    eventHandlerThreads.shutdown();
  }

  @Override
  public void run() {
    try {
      while (!stopped) {
        Event event = queue.take();
        String eventName = event.getName();
        for (Action action : subscriptionMap.get(eventName)) {
          ActionTask task = new ActionTask(action, event);
          CompletableFuture
              .supplyAsync(task::runTask)
              .thenAcceptAsync(
                  this::dispatchAll,
                  eventHandlerThreads) // use local threads instead of ForkJoinPool
              .whenCompleteAsync((v, e) -> {
                if (e != null) {
                  LOG.error("Error when running action task: ", e);
                }
              }, eventHandlerThreads);
        }
      }
    } catch (InterruptedException e) {
      LOG.info("EventHandler has been interrupted, terminating...");
    }
  }

  private class ActionTask {

    private Action action;
    private Event event;

    ActionTask(Action action, Event event) {
      this.action = action;
      this.event = event;
    }

    public Collection<Event> runTask() {

      /* synchronizing on the Action instance for thread-safety
         (i.e. an action under a cluster should not be executed concurrently) */
      synchronized (action) {
        try {
          return action.on(event);
        } catch (Exception e) {
          throw new CompletionException("Failed to execute action " + action.getClass(), e);
        }
      }
    }

  }

}
