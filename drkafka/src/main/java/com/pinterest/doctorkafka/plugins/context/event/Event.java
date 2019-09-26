package com.pinterest.doctorkafka.plugins.context.event;

import com.pinterest.doctorkafka.plugins.context.Context;

/**
 *
 * Events are messages that are emitted by plugins to an {@link EventDispatcher}
 * which will trigger registered {@link com.pinterest.doctorkafka.plugins.action.Action Actions}.
 *
 * <pre>
                                                                     +-------+
                                                             +------+| Event |<----+
                                                             |       +-------+     |
                                                             |                     |
                                                             |                     |
                                                             v                     +
 +--------------+                 +--------------+    +---------------+    +---------------+
 |              |    +-------+    |              |    |               |    |               |
 |   Operator   |--->| Event |--->| EventEmitter |--->|EventDispatcher|--->|    Action     |
 |              |    +-------+    |              |    |               |    |               |
 +--------------+                 +--------------+    +---------------+    +---------------+
                                                                     execute
 * </pre>
 */
public abstract class Event extends Context {
  private String name;

  public final void setName(String name){
    this.name = name;
  }

  public final String getName(){
    return this.name;
  }
}
