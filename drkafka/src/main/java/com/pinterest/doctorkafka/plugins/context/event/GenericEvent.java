package com.pinterest.doctorkafka.plugins.context.event;

import java.util.Map;

public class GenericEvent extends Event {
  public GenericEvent(){}
  public GenericEvent(String name, Map<String, Object> attributes){
    super.setName(name);
    if (attributes != null){
      for (Map.Entry<String, Object> entry : attributes.entrySet()){
        super.setAttribute(entry.getKey(), entry.getValue());
      }
    }
  }
}
