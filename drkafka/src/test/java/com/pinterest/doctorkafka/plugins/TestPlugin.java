package com.pinterest.doctorkafka.plugins;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.pinterest.doctorkafka.plugins.utils.DummyStub;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TestPlugin {
  private static final String TEST_KEY = "test.key";
  DummyStub spyObj1 = spy(new DummyStub());

  Plugin plugin = new Plugin() {
    @Override
    public void configure(ImmutableConfiguration config) {
      DummyStub dummyStub = config.get(DummyStub.class, TEST_KEY);
      dummyStub.dummy();
    }

    @Override
    public void initialize(ImmutableConfiguration config) {
      configure(config);
    }
  };

  @Test
  void testConfigureWithOneConfig() throws Exception{
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(TEST_KEY, spyObj1);
    AbstractConfiguration config = new MapConfiguration(configMap);

    plugin.initialize(config);

    verify(spyObj1,times(1)).dummy();
  }

}