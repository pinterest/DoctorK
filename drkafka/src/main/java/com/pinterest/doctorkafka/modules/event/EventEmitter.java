package com.pinterest.doctorkafka.modules.event;

public interface EventEmitter {
  void emit(Event event) throws Exception;
}
