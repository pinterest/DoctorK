package com.pinterest.doctorkafka.modules.event;

import com.pinterest.doctorkafka.modules.action.Action;

public interface EventListener {
  void receive(Event event) throws Exception;
  void subscribe(String eventName, Action action) throws Exception;
}
