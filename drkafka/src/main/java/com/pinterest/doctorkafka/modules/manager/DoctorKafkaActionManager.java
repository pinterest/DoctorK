package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.action.Action;
import com.pinterest.doctorkafka.modules.action.ReportOperation;
import com.pinterest.doctorkafka.modules.action.SendEvent;
import com.pinterest.doctorkafka.modules.action.cluster.ReplaceInstance;
import com.pinterest.doctorkafka.modules.action.cluster.kafka.ReassignPartition;

import org.apache.commons.configuration2.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DoctorKafkaActionManager extends ModuleManager implements ActionManager{
  private static final Logger LOG = LogManager.getLogger(DoctorKafkaMonitorManager.class);
  public static final String ACTION_SEND_EVENT_NAME = "send_event";
  public static final String ACTION_REPORT_OPERATION_NAME = "report_operation";
  public static final String ACTION_REASSIGN_PARTITION_NAME = "reassign_partition";
  public static final String ACTION_REPLACE_INSTANCE_NAME = "replace_instance";

  public DoctorKafkaActionManager(Configuration config) throws Exception {
    super(config);
  }

  @Override
  public SendEvent getSendEvent(Configuration additionalConfig) throws Exception {
    return (SendEvent) getActionByName(ACTION_SEND_EVENT_NAME, additionalConfig);
  }

  @Override
  public ReportOperation getReportOperation(Configuration additionalConfig) throws Exception {
    return (ReportOperation) getActionByName(ACTION_REPORT_OPERATION_NAME, additionalConfig);
  }

  @Override
  public ReassignPartition getReassignPartition(Configuration additionalConfig) throws Exception{
    return (ReassignPartition) getActionByName(ACTION_REASSIGN_PARTITION_NAME, additionalConfig);
  }

  @Override
  public ReplaceInstance getReplaceInstance(Configuration additionalConfig) throws Exception {
    return (ReplaceInstance) getActionByName(ACTION_REPLACE_INSTANCE_NAME, additionalConfig);
  }

  protected Action getActionByName(String name, Configuration additionalConfigs) throws Exception{
    Action action;
    try {
      Class<?> clazz = getModuleClass(name);
      Configuration config = getDefaultConfig(name);
      action = (Action) getConfiguredModule(clazz, additionalConfigs, config);
    } catch (Exception e){
      LOG.error("Failed to load action module {}. ", name, e);
      throw e;
    }
    return action;
  }
}
