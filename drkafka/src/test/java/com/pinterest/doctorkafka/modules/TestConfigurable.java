package com.pinterest.doctorkafka.modules;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.pinterest.doctorkafka.modules.utils.DummyStub;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TestConfigurable {
  private static final String TEST_KEY = "test.key";
  DummyStub spyObj1 = spy(new DummyStub());
  DummyStub spyObj2 = spy(new DummyStub());

  Configurable configurable = new Configurable() {
    @Override
    public void configure(AbstractConfiguration config) {
      DummyStub dummyStub = config.get(DummyStub.class, TEST_KEY);
      dummyStub.dummy();
    }
  };

  @Test
  void testConfigureWithOneConfig() throws Exception{
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(TEST_KEY, spyObj1);
    AbstractConfiguration config = new MapConfiguration(configMap);

    configurable.configure(config);

    verify(spyObj1,times(1)).dummy();
  }

  @Test
  void testConfigureWithMultipleConfig() throws Exception{
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(TEST_KEY, spyObj1);
    AbstractConfiguration config = new MapConfiguration(configMap);

    Map<String, Object> configMap1 = new HashMap<>();
    configMap.put(TEST_KEY, spyObj2);
    AbstractConfiguration config1 = new MapConfiguration(configMap1);

    configurable.configure(config1, config);
    verify(spyObj1,never()).dummy();
    verify(spyObj2,times(1)).dummy();
  }

}