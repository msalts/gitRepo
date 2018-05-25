package com.ph.coding.messaging;

/**
 * Note that wildcard publications are not supported!
 */
public interface MessageSender
{
  /**
   * @return This sender's topic.
   */
  String getTopic();

  /**
   * @return Whether the sender is started and can be used to send messages.
   */
  boolean isStarted();

  /**
   * Starts the sender. This should be called before any messages are sent.
   *
   * @throws MessagingException If the sender cannot be started.
   */
  void start() throws MessagingException;

  /**
   * Stops the sender and relinquishes internal resources.
   *
   * @throws MessagingException If some or all of the internal resources cannot be relinquished.
   */
  void stop() throws MessagingException;

  /**
   * Sends a message.
   *
   * @param message The message to send.
   * @throws MessagingException If there is a message transport problem.
   */
  void sendMessage(byte[] message) throws MessagingException;
}
