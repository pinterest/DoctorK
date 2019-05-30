package com.pinterest.doctorkafka.modules.monitor;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.context.Context;
import com.pinterest.doctorkafka.modules.state.State;

public interface Monitor extends Configurable {
  State observe(Context ctx, State state) throws Exception;
}
