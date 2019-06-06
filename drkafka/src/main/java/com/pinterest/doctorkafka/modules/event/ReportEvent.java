package com.pinterest.doctorkafka.modules.event;

public class ReportEvent extends GenericEvent {
  private static final String EVENT_SUBJECT_KEY = "subject";
  private static final String EVENT_MESSAGE_KEY = "message";
  public ReportEvent(String eventName, String subject, String message){
    super.setName(eventName);
    super.setAttribute(EVENT_SUBJECT_KEY , subject);
    super.setAttribute(EVENT_MESSAGE_KEY, message);
  }
}
