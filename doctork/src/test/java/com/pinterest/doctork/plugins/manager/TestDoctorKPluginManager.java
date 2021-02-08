package com.pinterest.doctork.plugins.manager;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.pinterest.doctork.plugins.Plugin;
import com.pinterest.doctork.plugins.errors.PluginConfigurationException;
import com.pinterest.doctork.plugins.errors.PluginException;
import com.pinterest.doctork.plugins.utils.DummyStub;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class TestDoctorKPluginManager {

  @Test
  public void testGetPlugin() throws Exception{
    DummyStub spyDummyStub = spy(new DummyStub());
    AbstractConfiguration config = new MapConfiguration(new HashMap<>());

    DoctorKPluginManager pluginManager = new DoctorKPluginManager();
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
      config.setProperty("class", BadPluginStub.class.getName());
      pluginManager.getPlugin(config);
      fail("Should fail since class has no nullary constructor");
    } catch (Exception e){
      assertTrue(e instanceof InstantiationException);
    }

    config.setProperty("class", PluginStub.class.getName());
    config.setProperty("config.test", spyDummyStub);
    pluginManager.getPlugin(config);
    verify(spyDummyStub,times(1)).dummy();
  }

  public static class PluginStub implements Plugin {
    @Override
    public void configure(ImmutableConfiguration config){
      config.get(DummyStub.class, "test").dummy();
    }

    @Override
    public void initialize(ImmutableConfiguration config){
      configure(config);
    }
  }

  public static class BadPluginStub implements Plugin {
    BadPluginStub(Object someArg){};

    @Override
    public void initialize(ImmutableConfiguration config) throws PluginConfigurationException {
      configure(config);
    }
  }

}