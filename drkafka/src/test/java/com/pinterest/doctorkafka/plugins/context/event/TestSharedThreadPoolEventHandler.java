package com.pinterest.doctorkafka.plugins.context.event;

import static org.junit.jupiter.api.Assertions.*;

import com.pinterest.doctorkafka.plugins.action.Action;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSharedThreadPoolEventHandler {
  private static final String TEST_EVENT = "test_event";
  private static final String TEST_EVENT_2 = "test_event_2";

  @Test
  public void testSharedThreadPoolEventHandler () throws Exception {

    Map<String, Object> confMap = new HashMap<>();
    confMap.put("dryrun", false);
    confMap.put("subscribed_events", "not important here since we're manually subscribing");
    AbstractConfiguration conf = new MapConfiguration(confMap);

    BlockingAction action1 = new BlockingAction();
    BlockingAction action2 = new BlockingAction();

    action1.initialize(conf);
    action2.initialize(conf);

    SharedThreadPoolEventHandler eventHandler = new SharedThreadPoolEventHandler();
    eventHandler.subscribe(TEST_EVENT, action1);
    eventHandler.subscribe(TEST_EVENT, action2);
    eventHandler.start();

    Event event = new GenericEvent(TEST_EVENT, null);
    eventHandler.emit(event);

    // 500 ms elapsed, both actions shouldn't have finished execution but should be started
    Thread.sleep(500);
    assertEquals(1, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());

    assertEquals(0, action1.exitCount.get());
    assertEquals(0, action2.exitCount.get());

    // 1.5 ms elapsed, both actions should finish
    Thread.sleep(1000);
    assertEquals(1, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());

    assertEquals(1, action1.exitCount.get());
    assertEquals(1, action2.exitCount.get());

    eventHandler.stop();
  }

  @Test
  public void testSharedThreadPoolEventHandlerChainedEvents () throws Exception {
    Map<String, Object> confMap = new HashMap<>();
    confMap.put("dryrun", false);
    confMap.put("subscribed_events", "not important here since we're manually subscribing");
    AbstractConfiguration conf = new MapConfiguration(confMap);

    BlockingAction action1 = new BlockingAction();
    BlockingChainAction action2 = new BlockingChainAction();
    BlockingAction action3 = new BlockingAction();

    action1.initialize(conf);
    action2.initialize(conf);
    action3.initialize(conf);

    SharedThreadPoolEventHandler eventHandler = new SharedThreadPoolEventHandler();
    eventHandler.subscribe(TEST_EVENT, action1);
    eventHandler.subscribe(TEST_EVENT, action2);
    eventHandler.subscribe(TEST_EVENT_2, action3);
    eventHandler.start();

    Event event = new GenericEvent(TEST_EVENT, null);
    eventHandler.emit(event);

    // 500ms elapsed, actions 1 & 2 should have started but not finished, action 3 not started
    Thread.sleep(500);
    assertEquals(1, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());
    assertEquals(0, action3.entryCount.get());

    assertEquals(0, action1.exitCount.get());
    assertEquals(0, action2.exitCount.get());
    assertEquals(0, action3.exitCount.get());

    // 1500 ms elapsed, action 1 & 2 should finish, action 3 should start but not finish
    Thread.sleep(1000);
    assertEquals(1, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());
    assertEquals(1, action3.entryCount.get());

    assertEquals(1, action1.exitCount.get());
    assertEquals(1, action2.exitCount.get());
    assertEquals(0, action3.exitCount.get());

    // 2500 ms elapsed, action 3 should finish
    Thread.sleep(1000);

    assertEquals(1, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());
    assertEquals(1, action3.entryCount.get());

    assertEquals(1, action1.exitCount.get());
    assertEquals(1, action2.exitCount.get());
    assertEquals(1, action3.exitCount.get());

    eventHandler.stop();
  }


  /**
   * We want to make sure that same action instances won't get executed concurrently
   *
   */
  @Test
  public void testConcurrentActionTaskSynchronization() throws Exception{
    Map<String, Object> confMap = new HashMap<>();
    confMap.put("dryrun", false);
    confMap.put("subscribed_events", "not important here since we're manually subscribing");
    AbstractConfiguration conf = new MapConfiguration(confMap);

    BlockingAction action1 = new BlockingAction();
    BlockingAction action2 = new BlockingAction();

    action1.initialize(conf);
    action2.initialize(conf);

    SharedThreadPoolEventHandler eventHandler = new SharedThreadPoolEventHandler();
    eventHandler.subscribe(TEST_EVENT, action1);
    eventHandler.subscribe(TEST_EVENT, action2);
    eventHandler.subscribe(TEST_EVENT_2, action1);
    eventHandler.start();

    Event event = new GenericEvent(TEST_EVENT, null);
    eventHandler.emit(event);

    // will trigger action1 again
    Event event2 = new GenericEvent((TEST_EVENT_2), null);
    Thread.sleep(100);
    eventHandler.emit(event2);

    // 500ms elapsed, action1 should be triggered only once, action2 should also be triggered but not finished
    Thread.sleep(400);
    assertEquals(1, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());

    assertEquals(0, action1.exitCount.get());
    assertEquals(0, action2.exitCount.get());

    // 1500ms elapsed, action1 should be triggered twice and finished once, action2 should be both triggered and finished once
    Thread.sleep(1000);
    assertEquals(2, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());

    assertEquals(1, action1.exitCount.get());
    assertEquals(1, action2.exitCount.get());

    // 2500ms elapsed, action1 should be both triggered and finished twice, action2 should be both triggered and finished once
    Thread.sleep(1000);
    assertEquals(2, action1.entryCount.get());
    assertEquals(1, action2.entryCount.get());

    assertEquals(2, action1.exitCount.get());
    assertEquals(1, action2.exitCount.get());

    eventHandler.stop();
  }

  private static class BlockingChainAction extends BlockingAction {
    @Override
    public Collection<Event> execute(Event event) throws Exception {
      super.execute(event);
      return Arrays.asList(new GenericEvent(TEST_EVENT_2, null), new GenericEvent("foo", null));
    }
  }

  private static class BlockingAction extends Action {
    AtomicInteger entryCount = new AtomicInteger(0);
    AtomicInteger exitCount = new AtomicInteger(0);

    @Override
    public Collection<Event> execute(Event event) throws Exception {
      entryCount.incrementAndGet();
      Thread.sleep(1000);
      exitCount.incrementAndGet();
      return null;
    }
  }

}