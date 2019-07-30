package com.pinterest.doctorkafka.config;


import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * kafkacluster.data07.dryrun=true
 * kafkacluster.data07.zkurl=datazk001:2181,datazk002:2181,/data07
 * kafkacluster.data07.peak_to_mean_ratio=3.5
 * kafkacluster.data07.threshold.cpu=0.32
 * kafkacluster.data07.threshold.network.inbound.mb=50
 * kafkacluster.data07.threshold.network.outbound.mb=150
 * kafkacluster.data07.threshold.disk.usage=0.75
 *
 */
public class DoctorKafkaClusterConfig {
  private static final String ZKURL = "zkurl";
  private static final String ENABLED = "enabled";

  private String clusterName;
  private String zkurl;

  private Map<String, Configuration> monitorConfigs = new LinkedHashMap<>();
  private Map<String, Configuration> operatorConfigs = new LinkedHashMap<>();
  private Map<String, Configuration> actionConfigs = new LinkedHashMap<>();

  public DoctorKafkaClusterConfig(String clusterName, HierarchicalConfiguration<?> configuration) {
    this.clusterName = clusterName;
    this.zkurl = configuration.getString(ZKURL);

    for(HierarchicalConfiguration monitorConfig: configuration.configurationsAt(DoctorKafkaConfig.MONITORS_PREFIX)){
      String name = monitorConfig.getString(DoctorKafkaConfig.NAME_KEY);
      monitorConfigs.put(name, monitorConfig);
    }

    for(HierarchicalConfiguration operatorConfig: configuration.configurationsAt(DoctorKafkaConfig.OPERATORS_PREFIX)){
      String name = operatorConfig.getString(DoctorKafkaConfig.NAME_KEY);
      operatorConfigs.put(name, operatorConfig);
    }

    for(HierarchicalConfiguration actionConfig: configuration.configurationsAt(DoctorKafkaConfig.ACTIONS_PREFIX)){
      String name = actionConfig.getString(DoctorKafkaConfig.NAME_KEY);
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
    return getEnabledModules(baseMonitorConfigs, monitorConfigs);
  }

  public Map<String, AbstractConfiguration> getEnabledOperatorsConfigs(Map<String, Configuration> baseOperatorConfigs) {
    return getEnabledModules(baseOperatorConfigs, operatorConfigs);
  }

  public Map<String, AbstractConfiguration> getEnabledActionsConfigs(Map<String, Configuration> baseActionConfigs) {
    return getEnabledModules(baseActionConfigs, actionConfigs);
  }

  protected Map<String, AbstractConfiguration> getEnabledModules(
      Map<String, Configuration> baseModuleConfigs,
      Map<String, Configuration> additionalModuleConfigs){
    Map<String, AbstractConfiguration> clusterModules = new LinkedHashMap<>();
    for(Map.Entry<String, Configuration> entry : baseModuleConfigs.entrySet()){
      String name = entry.getKey();
      if(isModuleEnabled(additionalModuleConfigs, name)){
        CompositeConfiguration newConfig = new CompositeConfiguration();
        if(additionalModuleConfigs.containsKey(name)){
          newConfig.addConfiguration(additionalModuleConfigs.get(name));
        }
        newConfig.addConfiguration(entry.getValue());
        clusterModules.put(name, newConfig);
      }
    }
    return clusterModules;
  }

  protected boolean isModuleEnabled(Map<String, Configuration> configs, String moduleName){
    return ( !configs.containsKey(moduleName) ||
             !configs.get(moduleName).containsKey(ENABLED) ||
             configs.get(moduleName).getString(ENABLED).equals("true"));
  }
}
