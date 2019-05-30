package com.pinterest.doctorkafka.modules.action;

public interface ReportOperation extends Action {
  void report(String entity, String message) throws Exception;
}
