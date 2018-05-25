package com.ph.coding.messaging;

/**
 * Provides singleton bootstrap and shutdown operations for a messaging provider within an application and acts
 * as a factory for {@code MessageSender} and {@code MessageReceiver} instances.
 */
public interface MessagingFactory
{
  /**
   * Start up any underlying resources necessary to facilitate messaging.
   * <p/>
   * Only ever called once at application startup.
   *
   * @throws Exception If the messaging infrastructure cannot be started.
   */
  void start() throws Exception;

  /**
   * Stop any underlying resources in advance of the application shutting down.
   * <p/>
   * Only ever called once at application shutdown.
   *
   * @throws Exception If the messaging infrastructure cannot be shutdown. There isn't much that can be done by the caller
   *         in this case other than to log the problem.
   */
  void shutdown() throws Exception;

  /**
   * @return A readable name for the messaging provider.
   */
  String getProviderName();

  /**
   * Creates a message sender on the given topic.
   *
   * @param topic The topic.
   * @return A new message sender.
   * @throws MessagingException If the factory is not correctly configured such that this cannot be achieved.
   */
  MessageSender createSender(String topic) throws MessagingException;

  /**
   * Create a message receiver on the given topic.
   *
   * @param topic The topic.
   * @return A new message receiver.
   * @throws MessagingException If the factory is not correctly configured such that this cannot be achieved.
   */
  MessageReceiver createReceiver(String topic) throws MessagingException;
}
