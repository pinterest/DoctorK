package com.pinterest.doctorkafka.modules.state;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class State {
  private AtomicBoolean stopped = new AtomicBoolean(false);

  public boolean isStopped() {
    return stopped.get();
  }
  public void stop() {
    stopped.set(true);
  }
}
