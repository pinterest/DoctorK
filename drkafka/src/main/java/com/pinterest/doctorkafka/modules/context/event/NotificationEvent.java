package com.pinterest.doctorkafka.modules.context.event;

/**
 * This event is a helper for notification actions
 */
public class NotificationEvent extends GenericEvent {
  public NotificationEvent(String eventName, String title, String message){
    super.setName(eventName);
    super.setAttribute(EventUtils.EVENT_TITLE_KEY, title);
    super.setAttribute(EventUtils.EVENT_MESSAGE_KEY, message);
  }
}
