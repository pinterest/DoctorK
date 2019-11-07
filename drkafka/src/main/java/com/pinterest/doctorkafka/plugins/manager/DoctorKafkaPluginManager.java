package com.pinterest.doctorkafka.plugins.manager;

import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.plugins.Plugin;
import com.pinterest.doctorkafka.plugins.errors.PluginException;
import com.pinterest.doctorkafka.plugins.monitor.Monitor;
import com.pinterest.doctorkafka.plugins.operator.Operator;
import com.pinterest.doctorkafka.plugins.task.TaskHandler;

import org.apache.commons.configuration2.AbstractConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create plugins based on configuration values and caches the classes for future instance creation
 * <br>
 * See {@link PluginManager}.
 */
public class DoctorKafkaPluginManager implements PluginManager {
  private static final String MODULE_CLASS_KEY = "class";
  private static final String MODULE_CONFIG_KEY = "config";
  private Map<String, Class> pluginMap = new ConcurrentHashMap<>();

  @Override
  public Monitor getMonitor(AbstractConfiguration monitorConfig) throws Exception {
    return (Monitor) getPlugin(monitorConfig);
  }

  @Override
  public Operator getOperator(AbstractConfiguration operatorConfig) throws Exception {
    return (Operator) getPlugin(operatorConfig);
  }

  @Override
  public TaskHandler getAction(AbstractConfiguration actionConfig) throws Exception {
    return (TaskHandler) getPlugin(actionConfig);
  }

  protected Plugin getPlugin(AbstractConfiguration pluginConfig) throws Exception {
    String pluginName = pluginConfig.getString(DoctorKafkaConfig.NAME_KEY);
    if(pluginName == null) {
      throw new PluginException("Could not find name in plugin config");
    }
    String pluginClass = pluginConfig.getString(MODULE_CLASS_KEY);
    if (pluginClass == null) {
      throw new PluginException("Could not find class in plugin config: " + pluginName);
    }
    Class<?> clazz = pluginMap.computeIfAbsent(pluginClass, c -> {
      try{
        return Class.forName(pluginClass);
      } catch (ClassNotFoundException e) {
        return null;
      }
    });
    if (clazz == null){
      throw new ClassNotFoundException("Could not find class in classpath for plugin " + pluginName + " (" + pluginClass + ")");
    }

    Plugin plugin = clazz.asSubclass(Plugin.class).newInstance();

    plugin.initialize(pluginConfig.immutableSubset(MODULE_CONFIG_KEY));
    return plugin;
  }
}
