package com.ph.coding.messaging;

/**
 * Note that wildcard subscriptions are not supported!
 */
public interface MessageReceiver
{
  /**
   * @return This receiver's topic.
   */
  String getTopic();

  /**
   * @return Whether the receiver is started and can be used to receive messages.
   */
  boolean isStarted();

  /**
   * Starts the receiver. This should be called before any messages will be received.
   *
   * @throws MessagingException If the receiver cannot be started.
   */
  void start() throws MessagingException;

  /**
   * Stops the receiver and relinquishes internal resources.
   *
   * @throws MessagingException If some or all of the internal resources cannot be relinquished.
   */
  void stop() throws MessagingException;

  /**
   * Set the listener to be called when a message arrives. Only one listener is allowed per receiver, it is up to
   * implementations whether they allow it to be re-assigned, although it is intended this will be called only once.
   *
   * @param listener Listener for messages.
   */
  void setListener(MessageReceiveListener listener);
}
