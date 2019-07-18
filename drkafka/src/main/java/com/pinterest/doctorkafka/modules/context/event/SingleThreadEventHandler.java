package com.pinterest.doctorkafka.modules.context.event;

import com.pinterest.doctorkafka.modules.action.Action;

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

public class SingleThreadEventHandler implements EventEmitter, EventListener, Runnable {
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
  public void receive(Event event) {
    if(event != null){
      queue.add(event);
    }
  }

  protected void receiveAll(Collection<Event> events) {
    if (events != null) {
      queue.addAll(events);
    }
  }

  @Override
  public void emit(Event event) {
    receive(event);
  }

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
            receiveAll(newEvents);
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
