package com.pinterest.doctork.config;


import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

public class DoctorKClusterConfig {
  private static final String ZKURL = "zkurl";
  private static final String ENABLED = "enabled";

  private String clusterName;
  private String zkurl;

  private Map<String, Configuration> monitorConfigs = new LinkedHashMap<>();
  private Map<String, Configuration> operatorConfigs = new LinkedHashMap<>();
  private Map<String, Configuration> actionConfigs = new LinkedHashMap<>();

  public DoctorKClusterConfig(String clusterName, HierarchicalConfiguration<?> configuration) {
    this.clusterName = clusterName;
    this.zkurl = configuration.getString(ZKURL);

    for(HierarchicalConfiguration monitorConfig: configuration.configurationsAt(DoctorKConfig.MONITORS_PREFIX)){
      String name = monitorConfig.getString(DoctorKConfig.NAME_KEY);
      monitorConfigs.put(name, monitorConfig);
    }

    for(HierarchicalConfiguration operatorConfig: configuration.configurationsAt(DoctorKConfig.OPERATORS_PREFIX)){
      String name = operatorConfig.getString(DoctorKConfig.NAME_KEY);
      operatorConfigs.put(name, operatorConfig);
    }

    for(HierarchicalConfiguration actionConfig: configuration.configurationsAt(DoctorKConfig.ACTIONS_PREFIX)){
      String name = actionConfig.getString(DoctorKConfig.NAME_KEY);
      actionConfigs.put(name, actionConfig);
    }
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getZkUrl() {
    return this.zkurl;
  }

  public Map<String, AbstractConfiguration> getEnabledMonitorsConfigs(Map<String, Configuration> baseMonitorConfigs) {
    return getEnabledPlugins(baseMonitorConfigs, monitorConfigs);
  }

  public Map<String, AbstractConfiguration> getEnabledOperatorsConfigs(Map<String, Configuration> baseOperatorConfigs) {
    return getEnabledPlugins(baseOperatorConfigs, operatorConfigs);
  }

  public Map<String, AbstractConfiguration> getEnabledActionsConfigs(Map<String, Configuration> baseActionConfigs) {
    return getEnabledPlugins(baseActionConfigs, actionConfigs);
  }

  protected Map<String, AbstractConfiguration> getEnabledPlugins(
      Map<String, Configuration> basePluginConfigs,
      Map<String, Configuration> additionalPluginConfigs){
    Map<String, AbstractConfiguration> clusterPlugins = new LinkedHashMap<>();
    for(Map.Entry<String, Configuration> entry : basePluginConfigs.entrySet()){
      String name = entry.getKey();
      if(isPluginEnabled(additionalPluginConfigs, name)){
        CompositeConfiguration newConfig = new CompositeConfiguration();
        if(additionalPluginConfigs.containsKey(name)){
          newConfig.addConfiguration(additionalPluginConfigs.get(name));
        }
        newConfig.addConfiguration(entry.getValue());
        clusterPlugins.put(name, newConfig);
      }
    }
    return clusterPlugins;
  }

  protected boolean isPluginEnabled(Map<String, Configuration> configs, String pluginName){
    return ( !configs.containsKey(pluginName) ||
             !configs.get(pluginName).containsKey(ENABLED) ||
             configs.get(pluginName).getString(ENABLED).equals("true"));
  }
}
