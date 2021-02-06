package com.pinterest.doctork.plugins.task;

import com.pinterest.doctork.plugins.context.Context;

/**
 *
 * Tasks are messages that are emitted by plugins to an {@link TaskDispatcher}
 * which will trigger registered {@link TaskHandler Actions}.
 *
 * <pre>
                                                                     +-------+
                                                             +------+| Task  |<----+
                                                             |       +-------+     |
                                                             |                     |
                                                             |                     |
                                                             v                     +
 +--------------+                 +--------------+    +---------------+    +---------------+
 |              |    +-------+    |              |    |               |    |               |
 |   Operator   |--->| Task  |--->| TaskEmitter  |--->|TaskDispatcher |--->|    Action     |
 |              |    +-------+    |              |    |               |    |               |
 +--------------+                 +--------------+    +---------------+    +---------------+
                                                                     execute
 * </pre>
 */
public abstract class Task extends Context {
  private String name;

  public final void setName(String name){
    this.name = name;
  }

  public final String getName(){
    return this.name;
  }
}
