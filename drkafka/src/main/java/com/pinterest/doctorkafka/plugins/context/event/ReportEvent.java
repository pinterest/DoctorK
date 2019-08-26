package com.pinterest.doctorkafka.plugins.context.event;

/**
 * This event is a helper for logging events
 */
public class ReportEvent extends GenericEvent {
  public ReportEvent(String eventName, String subject, String message){
    super.setName(eventName);
    super.setAttribute(EventUtils.EVENT_SUBJECT_KEY , subject);
    super.setAttribute(EventUtils.EVENT_MESSAGE_KEY, message);
  }
}
