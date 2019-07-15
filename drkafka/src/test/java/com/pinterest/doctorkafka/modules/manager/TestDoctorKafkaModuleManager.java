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
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class TestDoctorKafkaModuleManager {

  @Test
  public void testGetModule() throws Exception{
    DummyStub spyDummyStub = spy(new DummyStub());
    AbstractConfiguration config = new MapConfiguration(new HashMap<>());

    DoctorKafkaModuleManager moduleManager = new DoctorKafkaModuleManager();
    try {
      moduleManager.getModule(config);
      fail("Should fail since name not provided in the config");
    } catch (Exception e){
      assertTrue(e instanceof ModuleException);
    }
    try{
      config.setProperty("name","test");
      moduleManager.getModule(config);
      fail("Should fail since class not provided in the config");
    } catch (Exception e){
      assertTrue(e instanceof ModuleException);
    }

    try{
      config.setProperty("class", "bad.module.classpath");
      moduleManager.getModule(config);
      fail("Should fail since class in config is a invalid class name");
    } catch (Exception e){
      assertTrue(e instanceof ClassNotFoundException);
    }

    try {
      config.setProperty("class", this.getClass().getName());
      moduleManager.getModule(config);
      fail("Should fail since class is not a Configurable");
    } catch (Exception e){
      assertTrue(e instanceof ClassCastException);
    }

    try {
      config.setProperty("class", BadConfigurableStub.class.getName());
      moduleManager.getModule(config);
      fail("Should fail since class has no nullary constructor");
    } catch (Exception e){
      assertTrue(e instanceof InstantiationException);
    }

    config.setProperty("class", ConfigurableStub.class.getName());
    config.setProperty("config.test", spyDummyStub);
    moduleManager.getModule(config);
    verify(spyDummyStub,times(1)).dummy();
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