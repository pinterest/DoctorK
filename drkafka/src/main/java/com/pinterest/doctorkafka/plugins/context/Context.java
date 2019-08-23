package com.pinterest.doctorkafka.plugins.context;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Contexts are key-value objects that keeps attributes for plugins to access.
 */
public abstract class Context {
  private Map<String, Object> attributes = new ConcurrentHashMap<>();
  public Object getAttribute(String key){
    return attributes.get(key);
  }
  public void setAttribute(String key, Object value){
    attributes.put(key, value);
  }
  public boolean containsAttribute(String key) {return attributes.containsKey(key); }
}
