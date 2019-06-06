package com.pinterest.doctorkafka.modules.event;

import com.pinterest.doctorkafka.modules.context.Context;

public abstract class Event extends Context {
  private String name;

  public final void setName(String name){
    this.name = name;
  }

  public final String getName(){
    return this.name;
  }
}
