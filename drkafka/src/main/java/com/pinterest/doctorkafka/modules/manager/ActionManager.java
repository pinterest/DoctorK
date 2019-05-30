package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.action.ReportOperation;
import com.pinterest.doctorkafka.modules.action.SendEvent;
import com.pinterest.doctorkafka.modules.action.cluster.ReplaceInstance;
import com.pinterest.doctorkafka.modules.action.cluster.kafka.ReassignPartition;

import org.apache.commons.configuration2.Configuration;

public interface ActionManager {
  SendEvent getSendEvent(Configuration additionalConfig) throws Exception;
  ReportOperation getReportOperation(Configuration additionalConfig) throws Exception;
  ReassignPartition getReassignPartition(Configuration additionalConfig) throws Exception;
  ReplaceInstance getReplaceInstance(Configuration additionalConfig) throws Exception;
}
