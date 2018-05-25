package com.ph.coding.messaging;

public interface Message
{
  /**
   * @return The underlying message.
   */
  byte[] getMsg();

  /**
   * Dispose of any resources held by this message. This should be called by tha application once it has finished
   * processing the message.
   */
  void dispose();
}
