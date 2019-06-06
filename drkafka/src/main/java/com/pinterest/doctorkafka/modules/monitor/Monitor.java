package com.pinterest.doctorkafka.modules.monitor;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.state.State;

/**
 * A monitor module takes a {@link Context} and previous {@link State} to generate new {@link State}
 * for {@link com.pinterest.doctorkafka.modules.operator.Operator} and {@link com.pinterest.doctorkafka.modules.action.Action} modules to act on.
 */
public interface Monitor extends Configurable {

  /**
   * @param ctx Context of the service/cluster containing configs and the service/cluster's data
   * @param state State containing attributes of previous Monitors
   * @return New state that has the attributes added by this Monitor
   * @throws Exception
   */
  State observe(Context ctx, State state) throws Exception;
}
