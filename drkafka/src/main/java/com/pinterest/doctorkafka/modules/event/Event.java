package com.pinterest.doctorkafka.modules.event;

import com.pinterest.doctorkafka.modules.context.Context;

/**
 * Events are contexts that are emitted by modules to an {@link EventListener}
 * to trigger registered {@link com.pinterest.doctorkafka.modules.action.Action Actions}
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
