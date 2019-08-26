package com.pinterest.doctorkafka.plugins.monitor;

import com.pinterest.doctorkafka.plugins.Configurable;
import com.pinterest.doctorkafka.plugins.context.Context;
import com.pinterest.doctorkafka.plugins.context.state.State;

/**
 * A monitor plugin observes the external system, populates the {@link State} with derived attributes based on the observations it made.
 *
 * <pre>

             +------------+    +------------+
             |  External  |    |  External  |
             |  System(s) |    |  System(s) |
             +------------+    +------------+
                   +                 +
                   |                 |
                   v                 v
             +------------+    +------------+          +-----------+
  +-----+    |            |    |            |          |           |
  |State|+-->|  Monitor1  |+-->|  Monitor2  |+--....-->|Operator(s)|
  +-----+    |            |    |            |          |           |
             +------------+    +------------+          +-----------+

       observe            observe
 * </pre>
 */
public interface Monitor extends Configurable {

  /**
   * @param state State containing attributes of previous Monitors
   * @return New state that has the attributes added by this Monitor
   * @throws Exception
   */
  State observe(State state) throws Exception;
}
