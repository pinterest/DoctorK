package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.errors.ModuleException;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;

import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create modules based on configuration values and caches the classes for future instance creation
 */
public class DoctorKafkaModuleManager implements ModuleManager {
  private static final String MODULE_CLASS_KEY = "class";
  private static final String MODULE_CONFIG_KEY = "config";
  private Configuration monitorsConfig;
  private Configuration operatorsConfig;
  private Configuration actionsConfig;
  private Map<String, Class> moduleMap = new ConcurrentHashMap<>();

  public DoctorKafkaModuleManager(Configuration monitorsConfig,
                                  Configuration operatorsConfig,
                                  Configuration actionsConfig){
    this.monitorsConfig = monitorsConfig;
    this.operatorsConfig = operatorsConfig;
    this.actionsConfig = actionsConfig;
  }

  @Override
  public Monitor getMonitor(String name, Configuration additionalConfig) throws Exception {
    return (Monitor) getModule(name, monitorsConfig, additionalConfig);
  }

  @Override
  public Operator getOperator(String name, Configuration additionalConfig) throws Exception {
    return (Operator) getModule(name, operatorsConfig, additionalConfig);
  }

  @Override
  public Action getAction(String name, Configuration additionalConfig) throws Exception {
    return (Action) getModule(name, actionsConfig, additionalConfig);
  }

  protected Configurable getModule(String name, Configuration baseConfig, Configuration additionalModuleConfig) throws Exception {
    String moduleClass = baseConfig.getString(String.join(".",name,MODULE_CLASS_KEY));
    if (moduleClass == null){
      throw new ModuleException("Could not find class in plugin config: " + name);
    }
    Class<?> clazz = moduleMap.computeIfAbsent(moduleClass, c -> {
      try{
        return Class.forName(moduleClass);
      } catch (ClassNotFoundException e) {
        return null;
      }
    });
    if (clazz == null){
      throw new ClassNotFoundException("Could not find class in classpath for plugin " + name + " (" + moduleClass + ")");
    }

    Configuration config = baseConfig.subset(String.join(".", name, MODULE_CONFIG_KEY));

    Configurable configurable = clazz.asSubclass(Configurable.class).newInstance();
    configurable.configure(additionalModuleConfig, config);
    return configurable;
  }
}
