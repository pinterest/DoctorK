package com.pinterest.doctorkafka.plugins.manager;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.pinterest.doctorkafka.plugins.Configurable;
import com.pinterest.doctorkafka.plugins.errors.PluginException;
import com.pinterest.doctorkafka.plugins.utils.DummyStub;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class TestDoctorKafkaPluginManager {

  @Test
  public void testGetPlugin() throws Exception{
    DummyStub spyDummyStub = spy(new DummyStub());
    AbstractConfiguration config = new MapConfiguration(new HashMap<>());

    DoctorKafkaPluginManager pluginManager = new DoctorKafkaPluginManager();
    try {
      pluginManager.getPlugin(config);
      fail("Should fail since name not provided in the config");
    } catch (Exception e){
      assertTrue(e instanceof PluginException);
    }
    try{
      config.setProperty("name","test");
      pluginManager.getPlugin(config);
      fail("Should fail since class not provided in the config");
    } catch (Exception e){
      assertTrue(e instanceof PluginException);
    }

    try{
      config.setProperty("class", "bad.plugin.classpath");
      pluginManager.getPlugin(config);
      fail("Should fail since class in config is a invalid class name");
    } catch (Exception e){
      assertTrue(e instanceof ClassNotFoundException);
    }

    try {
      config.setProperty("class", this.getClass().getName());
      pluginManager.getPlugin(config);
      fail("Should fail since class is not a Configurable");
    } catch (Exception e){
      assertTrue(e instanceof ClassCastException);
    }

    try {
      config.setProperty("class", BadConfigurableStub.class.getName());
      pluginManager.getPlugin(config);
      fail("Should fail since class has no nullary constructor");
    } catch (Exception e){
      assertTrue(e instanceof InstantiationException);
    }

    config.setProperty("class", ConfigurableStub.class.getName());
    config.setProperty("config.test", spyDummyStub);
    pluginManager.getPlugin(config);
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