package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.errors.ModuleException;
import com.pinterest.doctorkafka.modules.monitor.Monitor;
import com.pinterest.doctorkafka.modules.operator.Operator;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create modules based on configuration values and caches the classes for future instance creation
 */
public class DoctorKafkaModuleManager implements ModuleManager {
  private static final String MODULE_CLASS_KEY = "class";
  private static final String MODULE_CONFIG_KEY = "config";
  private Map<String, Class> moduleMap = new ConcurrentHashMap<>();

  @Override
  public Monitor getMonitor(AbstractConfiguration monitorConfig) throws Exception {
    return (Monitor) getModule(monitorConfig);
  }

  @Override
  public Operator getOperator(AbstractConfiguration operatorConfig) throws Exception {
    return (Operator) getModule(operatorConfig);
  }

  @Override
  public Action getAction(AbstractConfiguration actionConfig) throws Exception {
    return (Action) getModule(actionConfig);
  }

  protected Configurable getModule(AbstractConfiguration moduleConfig) throws Exception {
    String moduleName = moduleConfig.getString(DoctorKafkaConfig.NAME_KEY);
    if(moduleName == null) {
      throw new ModuleException("Could not find name in plugin config");
    }
    String moduleClass = moduleConfig.getString(MODULE_CLASS_KEY);
    if (moduleClass == null) {
      throw new ModuleException("Could not find class in plugin config: " + moduleName);
    }
    Class<?> clazz = moduleMap.computeIfAbsent(moduleClass, c -> {
      try{
        return Class.forName(moduleClass);
      } catch (ClassNotFoundException e) {
        return null;
      }
    });
    if (clazz == null){
      throw new ClassNotFoundException("Could not find class in classpath for plugin " + moduleName + " (" + moduleClass + ")");
    }

    Configurable configurable = clazz.asSubclass(Configurable.class).newInstance();
    configurable.configure(moduleConfig.subset(MODULE_CONFIG_KEY));
    return configurable;
  }
}
