package com.pinterest.doctorkafka.plugins.monitor;

import com.pinterest.doctorkafka.plugins.Plugin;
import com.pinterest.doctorkafka.plugins.context.state.State;
import com.pinterest.doctorkafka.plugins.errors.PluginConfigurationException;

import org.apache.commons.configuration2.ImmutableConfiguration;

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
public abstract class Monitor implements Plugin {

  /**
   * @param state State containing attributes of previous Monitors
   * @return New state that has the attributes added by this Monitor
   * @throws Exception
   */
  public abstract State observe(State state) throws Exception;

  @Override
  public final void initialize(ImmutableConfiguration config) throws PluginConfigurationException {
    configure(config);
  }
}
