package com.pinterest.doctorkafka.modules.operator;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.state.State;

public interface Operator extends Configurable {
  // returns false if later operations should not be executed
  boolean operate(Context ctx, State state) throws Exception;
  boolean isDryRun();
  String getConfigName();
}
