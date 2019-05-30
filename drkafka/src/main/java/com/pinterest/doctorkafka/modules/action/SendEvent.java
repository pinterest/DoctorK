package com.pinterest.doctorkafka.modules.action;

public interface SendEvent extends Action {
  void alert(String title, String message) throws Exception;
  void notify(String title, String message) throws Exception;
}
