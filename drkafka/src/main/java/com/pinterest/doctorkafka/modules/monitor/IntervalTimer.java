package com.pinterest.doctorkafka.modules.monitor;

import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.modules.state.State;

import org.apache.commons.configuration2.AbstractConfiguration;

public class IntervalTimer implements Monitor {
  private static final String CONFIG_INTERVAL_SECONDS_KEY = "interval.seconds";

  long configIntervalSeconds = 5L;

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    configIntervalSeconds = config.getLong(CONFIG_INTERVAL_SECONDS_KEY, configIntervalSeconds);
  }

  @Override
  public State observe(Context ctx, State state) throws Exception {
    Thread.sleep(configIntervalSeconds * 1000L);
    return state;
  }
}
