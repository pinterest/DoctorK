package com.pinterest.doctorkafka.modules.event;

import static org.mockito.Mockito.*;

import com.pinterest.doctorkafka.modules.action.Action;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestEvent {

  @Test
  void testSingleSubscriptionEvent() throws Exception {
    Action mockAction = mock(Action.class);

    Event event = new GenericEvent("test_event",null);

    SingleThreadEventHandler eventHandler = new SingleThreadEventHandler();
    eventHandler.subscribe("test_event", mockAction);
    eventHandler.start();
    eventHandler.emit(event);

    Thread.sleep(1000);

    eventHandler.stop();

    verify(mockAction, times(1)).execute(event);
  }

  @Test
  void testMultipleSubscriptionEvents() throws Exception {
    Action mockAction = mock(Action.class);
    Action mockAction2 = mock(Action.class);

    Event event = new GenericEvent("test_event",null);

    SingleThreadEventHandler eventHandler = new SingleThreadEventHandler();
    eventHandler.subscribe("test_event", mockAction);
    eventHandler.subscribe("test_event", mockAction2);
    eventHandler.start();
    eventHandler.emit(event);

    Thread.sleep(1000);

    eventHandler.stop();

    verify(mockAction, times(1)).execute(event);
    verify(mockAction2, times(1)).execute(event);
  }

  @Test
  void testChainedEvents() throws Exception {
    Action mockAction = mock(Action.class);
    Action mockNextAction = mock(Action.class);
    Event event = new GenericEvent("test_event",null);
    Event event2 = new GenericEvent("test_next_event", null);

    when(mockAction.execute(event)).thenReturn(Arrays.asList(event2));

    SingleThreadEventHandler eventHandler = new SingleThreadEventHandler();
    eventHandler.subscribe("test_event", mockAction);
    eventHandler.subscribe("test_next_event", mockNextAction);
    eventHandler.start();
    eventHandler.emit(event);

    Thread.sleep(1000);

    eventHandler.stop();

    verify(mockAction, times(1)).execute(event);
    verify(mockNextAction, times(1)).execute(event2);
  }
}