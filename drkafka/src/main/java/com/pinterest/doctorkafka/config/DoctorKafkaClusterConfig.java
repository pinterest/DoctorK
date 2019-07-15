package com.pinterest.doctorkafka.config;


import static com.pinterest.doctorkafka.config.DoctorKafkaConfig.NAME_KEY;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
  private static final String MB_IN_PER_SECOND = "inbound_limit_mb";
  private static final String MB_OUT_PER_SECOND = "outbound_limit_mb";

  private String clusterName;
  private String zkurl;

  private Double bytesInPerSecond;
  private Double bytesOutPerSecond;

  private ConcurrentMap<String, Configuration> monitorConfigs = new ConcurrentHashMap<>();
  private ConcurrentMap<String, Configuration> operatorConfigs = new ConcurrentHashMap<>();
  private ConcurrentMap<String, Configuration> actionConfigs = new ConcurrentHashMap<>();

  public DoctorKafkaClusterConfig(String clusterName, HierarchicalConfiguration<ImmutableNode> configuration) {
    this.clusterName = clusterName;
    this.zkurl = configuration.getString(ZKURL);
    this.bytesInPerSecond = configuration.getDouble(MB_IN_PER_SECOND) * 1024 * 1024;
    this.bytesOutPerSecond = configuration.getDouble(MB_OUT_PER_SECOND) * 1024 * 1024;

    for(HierarchicalConfiguration monitorConfig: configuration.configurationsAt(DoctorKafkaConfig.MONITORS_PREFIX)){
      String name = monitorConfig.getString(NAME_KEY);
      monitorConfigs.put(name, monitorConfig);
    }

    for(HierarchicalConfiguration operatorConfig: configuration.configurationsAt(DoctorKafkaConfig.OPERATORS_PREFIX)){
      String name = operatorConfig.getString(NAME_KEY);
      operatorConfigs.put(name, operatorConfig);
    }

    for(HierarchicalConfiguration actionConfig: configuration.configurationsAt(DoctorKafkaConfig.ACTIONS_PREFIX)){
      String name = actionConfig.getString(NAME_KEY);
      actionConfigs.put(name, actionConfig);
    }
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getZkUrl() {
    return this.zkurl;
  }

  public Double getBytesInPerSecond() {
    return bytesInPerSecond;
  }

  public Double getBytesOutPerSecond() {
    return bytesOutPerSecond;
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
    Map<String, AbstractConfiguration> clusterModules = new HashMap<>();
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
