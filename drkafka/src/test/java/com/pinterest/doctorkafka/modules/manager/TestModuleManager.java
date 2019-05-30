package com.pinterest.doctorkafka.modules.manager;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.pinterest.doctorkafka.modules.Configurable;
import com.pinterest.doctorkafka.modules.errors.ModuleException;
import com.pinterest.doctorkafka.modules.utils.DummyStub;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TestModuleManager {

  @Test
  void testGetModuleClass() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("badclass.class","path.to.some.class");
    configMap.put("goodclass.class",this.getClass().getCanonicalName());
    AbstractConfiguration config = new MapConfiguration(configMap);

    ModuleManager moduleManager = new ModuleManager(config);
    try {
      moduleManager.getModuleClass("nonexistingclass");
    } catch (Exception e){
      assertTrue(e instanceof ModuleException);
    }

    try {
      moduleManager.getModuleClass("badclass");
      fail("Should not pass since path is invalid");
    } catch (Exception e){
      assertTrue(e instanceof ClassNotFoundException);
    }

    Class<?> clazz = moduleManager.getModuleClass("goodclass");
    assertEquals(TestModuleManager.class, clazz);
  }

  @Test
  void testGetConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("module.class","path.to.some.class");
    configMap.put("module.config.foo","bar");
    AbstractConfiguration config = new MapConfiguration(configMap);

    ModuleManager moduleManager = new ModuleManager(config);
    Configuration c = moduleManager.getDefaultConfig("fakemodulename");
    assertEquals(0, c.size());

    Configuration moduleConfig = moduleManager.getDefaultConfig("module");
    assertEquals("bar", moduleConfig.getString("foo"));
  }

  @Test
  void testGetConfiguredModule() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    DummyStub dummyStub = spy(new DummyStub());
    configMap.put("test",dummyStub);
    AbstractConfiguration config = new MapConfiguration(configMap);

    ModuleManager moduleManager = new ModuleManager(config);
    try {
      moduleManager.getConfiguredModule(TestModuleManager.class, config);
      fail("Should not pass since class is not a Configurable");
    } catch (Exception e){
      assertTrue(e instanceof ClassCastException);
    }

    try {
      moduleManager.getConfiguredModule(BadConfigurableStub.class, config);
      fail("Should not pass since class has no nullary constructor");
    } catch (Exception e){
      assertTrue(e instanceof InstantiationException);
    }

    Configurable c = moduleManager.getConfiguredModule(ConfigurableStub.class, config);
    assertTrue(c instanceof ConfigurableStub);
    verify(dummyStub, times(1)).dummy();

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