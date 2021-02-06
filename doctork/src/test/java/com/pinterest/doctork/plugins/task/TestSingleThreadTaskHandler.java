package com.pinterest.doctork.plugins.task;

import static org.mockito.Mockito.*;

import com.pinterest.doctork.plugins.task.SingleThreadTaskDispatcher;
import com.pinterest.doctork.plugins.task.Task;
import com.pinterest.doctork.plugins.task.TaskHandler;
import com.pinterest.doctork.plugins.task.cluster.GenericTask;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestSingleThreadTaskHandler {

  @Test
  void testSingleSubscriptionTask() throws Exception {
    TaskHandler mockAction = mock(TaskHandler.class);

    Task task = new GenericTask("test_task",null);

    SingleThreadTaskDispatcher taskHandler = new SingleThreadTaskDispatcher();
    taskHandler.subscribe("test_task", mockAction);
    taskHandler.start();
    taskHandler.emit(task);

    Thread.sleep(1000);

    taskHandler.stop();

    verify(mockAction, times(1)).execute(task);
  }

  @Test
  void testMultipleSubscriptionTasks() throws Exception {
    TaskHandler mockAction = mock(TaskHandler.class);
    TaskHandler mockAction2 = mock(TaskHandler.class);

    Task task = new GenericTask("test_task",null);

    SingleThreadTaskDispatcher taskHandler = new SingleThreadTaskDispatcher();
    taskHandler.subscribe("test_task", mockAction);
    taskHandler.subscribe("test_task", mockAction2);
    taskHandler.start();
    taskHandler.emit(task);

    Thread.sleep(1000);

    taskHandler.stop();

    verify(mockAction, times(1)).execute(task);
    verify(mockAction2, times(1)).execute(task);
  }

  @Test
  void testChainedTasks() throws Exception {
    TaskHandler mockAction = mock(TaskHandler.class);
    TaskHandler mockNextAction = mock(TaskHandler.class);
    Task task = new GenericTask("test_task",null);
    Task task2 = new GenericTask("test_next_task", null);

    when(mockAction.execute(task)).thenReturn(Arrays.asList(task2));

    SingleThreadTaskDispatcher taskHandler = new SingleThreadTaskDispatcher();
    taskHandler.subscribe("test_task", mockAction);
    taskHandler.subscribe("test_next_task", mockNextAction);
    taskHandler.start();
    taskHandler.emit(task);

    Thread.sleep(1000);

    taskHandler.stop();

    verify(mockAction, times(1)).execute(task);
    verify(mockNextAction, times(1)).execute(task2);
  }
}