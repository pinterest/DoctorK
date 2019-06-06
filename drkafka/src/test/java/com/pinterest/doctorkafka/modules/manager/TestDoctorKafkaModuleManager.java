package com.pinterest.doctorkafka.modules.manager;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.errors.ModuleException;
import com.pinterest.doctorkafka.modules.utils.DummyStub;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TestDoctorKafkaModuleManager {

  @Test
  public void testGetModule() throws Exception{
    DummyStub spyDummyStub = spy(new DummyStub());
    DummyStub spyDummyStubNotCalled = spy(new DummyStub());
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("noclasspathmodule.config.key","value");
    configMap.put("badclasspathmodule.class","bad.module.classpath");
    configMap.put("notaconfigurablemodule.class",this.getClass().getName());
    configMap.put("missingnoargctrmodule.class",BadConfigurableStub.class.getName());
    configMap.put("goodmodule.class",ConfigurableStub.class.getName());
    configMap.put("goodmodule.config.test",spyDummyStub);
    configMap.put("goodmodulewithadditionalconfig.class",ConfigurableStub.class.getName());
    configMap.put("goodmodulewithadditionalconfig.config.test",spyDummyStubNotCalled);
    Configuration config = new MapConfiguration(configMap);

    DoctorKafkaModuleManager moduleManager = new DoctorKafkaModuleManager(null,null,null);
    try{
      moduleManager.getModule("noclasspathmodule",config,null);
      fail();
    } catch (Exception e){
      assertTrue(e instanceof ModuleException);
    }

    try{
      moduleManager.getModule("badclasspathmodule", config, null);
      fail();
    } catch (Exception e){
      assertTrue(e instanceof ClassNotFoundException);
    }

    try {
      moduleManager.getModule("notaconfigurablemodule", config, null);
      fail();
    } catch (Exception e){
      assertTrue(e instanceof ClassCastException);
    }

    try {
      moduleManager.getModule("missingnoargctrmodule", config, null);
      fail();
    } catch (Exception e){
      assertTrue(e instanceof InstantiationException);
    }

    moduleManager.getModule("goodmodule", config, null);
    verify(spyDummyStub,times(1)).dummy();

    DummyStub spyDummyStubAdditional = spy(new DummyStub());
    Map<String, Object> additionalConfigMap = new HashMap<>();
    additionalConfigMap.put("test",spyDummyStubAdditional);
    Configuration additionalConfig = new MapConfiguration(additionalConfigMap);

    moduleManager.getModule("goodmodulewithadditionalconfig", config, additionalConfig);
    verify(spyDummyStubNotCalled, never()).dummy();
    verify(spyDummyStubAdditional,times(1)).dummy();
  }

  public static class ConfigurableStub implements Configurable {
    @Override
    public void configure(AbstractConfiguration config){
      config.get(DummyStub.class, "test").dummy();
    }
  }

  public static class BadConfigurableStub implements Configurable {
    BadConfigurableStub(Object someArg){}
  }

}