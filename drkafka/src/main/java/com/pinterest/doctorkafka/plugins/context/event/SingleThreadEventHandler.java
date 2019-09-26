package com.pinterest.doctorkafka.plugins.context.event;

import com.pinterest.doctorkafka.plugins.action.Action;
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
 * SingleThreadEventHandler is a event handler that implements a naive dispatcher of {@link Event Events}. It runs as
 * a pub-sub instance which delivers actions FIFO using a queue within a single thread. It also implements
 * the {@link EventEmitter} interface such that {@link Operator Operators} can emit Events by calling the emit function.
 */
public class SingleThreadEventHandler implements EventEmitter, EventDispatcher, Runnable {
  private static final Logger LOG = LogManager.getLogger(SingleThreadEventHandler.class);
  private ExecutorService executorService = Executors.newSingleThreadExecutor();
  private volatile boolean stopped = false;
  private Map<String, Set<Action>> subscriptionMap= new ConcurrentHashMap<>();
  private BlockingQueue<Event> queue = new LinkedBlockingQueue<>();

  @Override
  public void subscribe(String eventName, Action action) {
    subscriptionMap.computeIfAbsent(eventName, name -> new HashSet<>()).add(action);
    LOG.info("Action {} has subscribed to event {}", action.getClass(), eventName);
  }

  @Override
  public void dispatch(Event event) {
    if(event != null){
      queue.add(event);
    }
  }

  protected void dispatchAll(Collection<Event> events) {
    if (events != null) {
      queue.addAll(events);
    }
  }

  @Override
  public void emit(Event event) {
    dispatch(event);
  }

  /**
   * Start the event handler
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
        Event event = queue.take();
        String eventName = event.getName();
        for (Action action : subscriptionMap.get(eventName)){
          try {
            Collection<Event> newEvents = action.on(event);
            dispatchAll(newEvents);
          } catch (Exception e){
            LOG.error("Failed to execute action {}", action.getClass() , e);
          }
        }
      }
    } catch (InterruptedException e){
      LOG.info("EventHandler has been interrupted, terminating...");
    }
  }
}
