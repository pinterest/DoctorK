package com.pinterest.doctorkafka.modules.state;

import com.pinterest.doctorkafka.modules.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class State extends Context {
  private AtomicBoolean stopped = new AtomicBoolean(false);

  public boolean isStopped() {
    return stopped.get();
  }
  public void stop() {
    stopped.set(true);
  }
}
