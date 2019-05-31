package com.pinterest.doctorkafka.modules.manager;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class DoctorKafkaActionManagerTest {
  static DoctorKafkaActionManager doctorKafkaActionManager;
  @BeforeAll
  static void setUp() throws Exception{
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("nonexistingaction.class", "this.is.a.path.to.a.BadSendEvent");
    configMap.put("missingconfigaction.class", "com.pinterest.doctorkafka.modules.action.EmailSendEvent");
    configMap.put("goodaction.class", "com.pinterest.doctorkafka.modules.action.cluster.ScriptReplaceInstance");
    configMap.put("goodaction.config.script","echo 1 | /dev/null");
    Configuration config = new MapConfiguration(configMap);
    doctorKafkaActionManager = new DoctorKafkaActionManager(config);
  }

  @Test
  void testGetActionByName() throws Exception {
    try {
      doctorKafkaActionManager.getActionByName("nonexistingaction", null);
      fail("Should not pass since action class does not exist");
    } catch (Exception e){
      assertTrue(e instanceof ClassNotFoundException);
    }

    try {
      doctorKafkaActionManager.getActionByName("missingconfigaction", null);
      fail("Should not pass since action class config has missing required configs");
    } catch (Exception e){
      assertTrue(e instanceof ModuleConfigurationException);
    }

    Map<String, Object> additionalConfigMap = new HashMap<>();
    additionalConfigMap.put("notification.emails", "aaa@bbb.com");
    additionalConfigMap.put("alert.emails", "aaa@bbb.com");
    Configuration additionalConfigs = new MapConfiguration(additionalConfigMap);

    doctorKafkaActionManager.getActionByName("missingconfigaction", additionalConfigs);

    doctorKafkaActionManager.getActionByName("goodaction", null);
  }
}