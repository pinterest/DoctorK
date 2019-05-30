package com.pinterest.doctorkafka.modules.manager;

import com.pinterest.doctorkafka.modules.monitor.Monitor;

import org.apache.commons.configuration2.AbstractConfiguration;

public interface MonitorManager {
  Monitor getMonitor(String monitorKey, AbstractConfiguration additionalConfigs) throws Exception;
}
