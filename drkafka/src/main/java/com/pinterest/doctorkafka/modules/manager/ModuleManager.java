package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.errors.ModuleException;

import org.apache.commons.configuration2.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ModuleManager {
  private static final String MODULE_CLASS_KEY = "class";
  private static final String MODULE_CONFIG_KEY = "config";
  private Configuration modulesConfig;
  private Map<String, Class> moduleMap = new ConcurrentHashMap<>();

  public ModuleManager(Configuration moduleConfig){
    this.modulesConfig = moduleConfig;
  }

  public Class<?> getModuleClass(String name) throws Exception {
    String moduleClass = modulesConfig.getString(String.join(".",name,MODULE_CLASS_KEY));
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
    return clazz;
  }

  public Configuration getDefaultConfig(String name) {
    Configuration config = modulesConfig.subset(String.join(".", name, MODULE_CONFIG_KEY));
    return config;
  }

  public Configurable getConfiguredModule(Class<?> clazz, Configuration... configurations) throws Exception {
    Configurable configurable = clazz.asSubclass(Configurable.class).newInstance();
    configurable.configure(configurations);
    return configurable;
  }
}
