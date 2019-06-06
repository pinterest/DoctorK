package com.pinterest.doctorkafka.modules.event;

public class NotificationEvent extends GenericEvent {
  private static final String EVENT_TITLE_KEY = "title";
  private static final String EVENT_MESSAGE_KEY = "message";
  public NotificationEvent(String eventName, String title, String message){
    super.setName(eventName);
    super.setAttribute(EVENT_TITLE_KEY, title);
    super.setAttribute(EVENT_MESSAGE_KEY, message);
  }
}
