package com.ph.coding.messaging;

public interface MessageReceiveListener
{
  /**
   * Handle the given message.
   * <p/>
   * Implementations should call the message's {@link Message#dispose()} method once they are done with it.
   *
   * @param message Received message.
   * @param topic Topic on which received.
   */
  void onMessage(Message message, String topic);
}
