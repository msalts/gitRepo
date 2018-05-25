package com.ph.coding.messaging.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.ph.coding.messaging.Message;
import com.ph.coding.messaging.MessageReceiveListener;
import com.ph.coding.messaging.MessageReceiver;
import com.ph.coding.messaging.MessageSender;
import com.ph.coding.messaging.MessagingException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MemoryMessagingTest
{
  private MemoryMessagingFactory messagingFactory;

  @Before
  public void before() throws Exception
  {
    messagingFactory = new MemoryMessagingFactory();
    messagingFactory.start();
  }

  @After
  public void after() throws Exception
  {
    messagingFactory.shutdown();
  }

  // -------------------------------------------------------------------------------- //

  @Test
  public void simpleSendReceive() throws Exception
  {
    final MessageSender sender = startedSender("a");
    final StoringListener listener = startedReceiver("a");

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();
    checkTopics(listener, "a", 6);
    checkMessages(listener, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void noSendUnlessStarted() throws Exception
  {
    final MessageSender sender = messagingFactory.createSender("a");
    final StoringListener listener = startedReceiver("a");

    try
    {
      sender.sendMessage(encode("The"));
      fail();
    }
    catch(final MessagingException expected)
    {
    }
    waitForMessages();
    assertTrue(listener.messages.isEmpty());

    sender.start();
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    waitForMessages();
    checkTopics(listener, "a", 2);
    checkMessages(listener, "cat", "sat");

    sender.stop();
    try
    {
      sender.sendMessage(encode("on"));
      fail();
    }
    catch(final MessagingException expected)
    {
    }
    waitForMessages();
    checkTopics(listener, "a", 2);
    checkMessages(listener, "cat", "sat");

    sender.start();
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));
    waitForMessages();
    checkTopics(listener, "a", 4);
    checkMessages(listener, "cat", "sat", "the", "mat");
  }

  @Test
  public void noReceiveUnlessStarted() throws Exception
  {
    final MessageSender sender = startedSender("a");
    final StoringListener listener = new StoringListener();
    final MessageReceiver receiver = messagingFactory.createReceiver("a");
    receiver.setListener(listener);

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    waitForMessages();
    assertTrue(listener.messages.isEmpty());

    receiver.start();
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    waitForMessages();
    checkTopics(listener, "a", 3);
    checkMessages(listener, "sat", "on", "the");

    receiver.stop();
    sender.sendMessage(encode("mat"));
    waitForMessages();
    checkTopics(listener, "a", 3);
    checkMessages(listener, "sat", "on", "the");
  }
  
  
  @Test
  public void createReceiverWithoutListener() throws Exception
  {
	  final MessageReceiver receiver = createReceiverNoListener("a");
	  try {
		  receiver.start();
	  	fail();
	  } catch(MessagingException expected)
	  {
	  }
	  final StoringListener listener = new StoringListener();
	  receiver.setListener(listener);
	  receiver.start();
	  final MessageSender sender = startedSender("a");
	  sender.sendMessage(encode("test"));
	  waitForMessages();
	  checkTopics(listener, "a", 1);
	  checkMessages(listener, "test");
  }
  
  @Test
  public void changeListenersWhilstReceiving() throws Exception
  {
	  final MessageSender sender = startedSender("a");
	  final MessageReceiver receiver = createReceiverNoListener("a");
	  final StoringListener listener1 = new StoringListener();
	  receiver.setListener(listener1);
	  receiver.start();
	  
	  sender.sendMessage(encode("message1"));
	  waitForMessages();
	  checkTopics(listener1, "a", 1);
	  checkMessages(listener1, "message1");
	  final StoringListener listener2 = new StoringListener();
	  checkTopics(listener2, "", 0);
	  receiver.setListener(listener2);
	  sender.sendMessage(encode("message2"));
	  waitForMessages();
	  checkTopics(listener2, "a", 1);
	  checkMessages(listener2, "message2");
  }

  @Test
  public void manySendersSameTopic() throws Exception
  {
    final MessageSender sender1 = startedSender("a");
    final MessageSender sender2 = startedSender("a");
    final MessageSender sender3 = startedSender("a");
    final StoringListener listener = startedReceiver("a");

    sender1.sendMessage(encode("The"));
    sender1.sendMessage(encode("cat"));
    sender2.sendMessage(encode("sat"));
    sender3.sendMessage(encode("on"));
    sender3.sendMessage(encode("the"));
    sender2.sendMessage(encode("mat"));

    waitForMessages();

    // all messages received as all senders on the same topic
    checkTopics(listener, "a", 6);
    checkMessages(listener, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void manySendersDifferentTopics() throws Exception
  {
    final MessageSender sender1 = startedSender("a");
    final MessageSender sender2 = startedSender("b");
    final MessageSender sender3 = startedSender("c");
    final StoringListener listener = startedReceiver("b");

    sender1.sendMessage(encode("The"));
    sender1.sendMessage(encode("cat"));
    sender2.sendMessage(encode("sat"));
    sender3.sendMessage(encode("on"));
    sender3.sendMessage(encode("the"));
    sender2.sendMessage(encode("mat"));

    waitForMessages();

    // only messages sent on topic "b" will be received
    checkTopics(listener, "b", 2);
    checkMessages(listener, "sat", "mat");
  }

  @Test
  public void manyReceiversSameListenerSameTopic() throws Exception
  {
    final MessageSender sender = startedSender("a");
    final StoringListener listener = new StoringListener();
    startedReceiver("a", listener);
    startedReceiver("a", listener);
    startedReceiver("a", listener);

    // each listener is asynchronous, so have to wait between each send for messages to finish before sending the next,
    // other output order is non-deterministic

    sender.sendMessage(encode("The"));
    waitForMessages();
    sender.sendMessage(encode("cat"));
    waitForMessages();
    sender.sendMessage(encode("sat"));
    waitForMessages();
    sender.sendMessage(encode("on"));
    waitForMessages();
    sender.sendMessage(encode("the"));
    waitForMessages();
    sender.sendMessage(encode("mat"));
    waitForMessages();

    checkTopics(listener, "a", 18);
    checkMessages(listener, "The", "The", "The", "cat", "cat", "cat", "sat", "sat", "sat", "on", "on", "on", "the", "the", "the", "mat", "mat", "mat");
  }

  @Test
  public void manyReceiversDifferentListenersSameTopic() throws Exception
  {
    final MessageSender sender = startedSender("a");
    final StoringListener listener1 = startedReceiver("a");
    final StoringListener listener2 = startedReceiver("a");
    final StoringListener listener3 = startedReceiver("a");

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();

    // each receiver/listener gets each message
    checkTopics(listener1, "a", 6);
    checkTopics(listener2, "a", 6);
    checkTopics(listener3, "a", 6);
    checkMessages(listener1, "The", "cat", "sat", "on", "the", "mat");
    checkMessages(listener2, "The", "cat", "sat", "on", "the", "mat");
    checkMessages(listener3, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void manyReceiversSameListenerDifferentTopics() throws Exception
  {
    final MessageSender sender = startedSender("a");
    final StoringListener listener = new StoringListener();
    startedReceiver("a", listener);
    startedReceiver("b", listener);
    startedReceiver("c", listener);

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();

    // only receiver on topic "a" gets messages
    checkTopics(listener, "a", 6);
    checkMessages(listener, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void manyReceiversDifferentListenersDifferentTopics() throws Exception
  {
    final MessageSender sender = startedSender("a");
    final StoringListener listener1 = startedReceiver("a");
    final StoringListener listener2 = startedReceiver("b");
    final StoringListener listener3 = startedReceiver("c");

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();

    // only receiver/listener on topic "a" gets messages
    checkTopics(listener1, "a", 6);
    checkMessages(listener1, "The", "cat", "sat", "on", "the", "mat");
    assertTrue(listener2.messages.isEmpty());
    assertTrue(listener3.messages.isEmpty());
  }

  @Test
  public void manySendersAndReceivers() throws Exception
  {
    final MessageSender sender1 = startedSender("a");
    final MessageSender sender2 = startedSender("b");
    final MessageSender sender3 = startedSender("b");
    final MessageSender sender4 = startedSender("c");

    final StoringListener listener1 = new StoringListener();
    final StoringListener listener2 = new StoringListener();

    startedReceiver("a", listener1);
    startedReceiver("a", listener2);
    startedReceiver("b", listener2);

    // listener1 receives from "a" via first receiver
    // listener2 receives from "a" via second receiver
    //                     and "b" via third receiver

    sender1.sendMessage(encode("Three-1-1"));
    waitForMessages();
    sender1.sendMessage(encode("blind-1-1"));
    waitForMessages();
    sender1.sendMessage(encode("mices-1-1"));
    waitForMessages();
    sender4.sendMessage(encode("Three-4-1"));
    waitForMessages();
    sender4.sendMessage(encode("blind-4-1"));
    waitForMessages();
    sender4.sendMessage(encode("mices-4-1"));
    waitForMessages();
    sender2.sendMessage(encode("Three-2-2"));
    waitForMessages();
    sender2.sendMessage(encode("blind-2-2"));
    waitForMessages();
    sender2.sendMessage(encode("mices-2-2"));
    waitForMessages();
    sender1.sendMessage(encode("Three-1-2"));
    waitForMessages();
    sender1.sendMessage(encode("blind-1-2"));
    waitForMessages();
    sender1.sendMessage(encode("mices-1-2"));
    waitForMessages();
    sender3.sendMessage(encode("Three-3-2"));
    waitForMessages();
    sender3.sendMessage(encode("blind-3-2"));
    waitForMessages();
    sender3.sendMessage(encode("mices-3-2"));
    waitForMessages();
    sender1.sendMessage(encode("Three-1-3"));
    waitForMessages();
    sender1.sendMessage(encode("blind-1-3"));
    waitForMessages();
    sender1.sendMessage(encode("mices-1-3"));
    waitForMessages();
    sender3.sendMessage(encode("Three-3-3"));
    waitForMessages();
    sender3.sendMessage(encode("blind-3-3"));
    waitForMessages();
    sender3.sendMessage(encode("mices-3-3"));
    waitForMessages();

    checkTopics(listener1, "a", 9);
    checkMessages(listener1,
                  "Three-1-1", "blind-1-1", "mices-1-1",
                  "Three-1-2", "blind-1-2", "mices-1-2",
                  "Three-1-3", "blind-1-3", "mices-1-3");

    checkTopics(listener2,
                "a", "a", "a",
                "b", "b", "b",
                "a", "a", "a",
                "b", "b", "b",
                "a", "a", "a",
                "b", "b", "b");
    checkMessages(listener2,
                  "Three-1-1", "blind-1-1", "mices-1-1",
                  "Three-2-2", "blind-2-2", "mices-2-2",
                  "Three-1-2", "blind-1-2", "mices-1-2",
                  "Three-3-2", "blind-3-2", "mices-3-2",
                  "Three-1-3", "blind-1-3", "mices-1-3",
                  "Three-3-3", "blind-3-3", "mices-3-3");
  }

  @Test
  public void manySendersAndReceiversAcrossDifferentThreads() throws Exception
  {
    final MessageSender sender1 = startedSender("a");
    final MessageSender sender2 = startedSender("b");
    final MessageSender sender3 = startedSender("b");
    final MessageSender sender4 = startedSender("c");

    final StoringListener listener1 = new StoringListener();
    final StoringListener listener2 = new StoringListener();

    startedReceiver("a", listener1);
    startedReceiver("a", listener2);
    startedReceiver("b", listener2);

    // listener1 receives from "a" via first receiver
    // listener2 receives from "a" via second receiver
    //                     and "b" via third receiver

    final Thread thread1 = new Thread (() -> {
      try
      {
        sender1.sendMessage(encode("Three-1-1"));
        sender1.sendMessage(encode("blind-1-1"));
        sender1.sendMessage(encode("mices-1-1"));

        sender4.sendMessage(encode("Three-4-1"));
        sender4.sendMessage(encode("blind-4-1"));
        sender4.sendMessage(encode("mices-4-1"));
      }
      catch(final MessagingException e)
      {
        // oops, have to wait for the test to fail...
      }
    });

    final Thread thread2 = new Thread (() -> {
      try
      {
        sender2.sendMessage(encode("Three-2-2"));
        sender2.sendMessage(encode("blind-2-2"));
        sender2.sendMessage(encode("mices-2-2"));

        sender1.sendMessage(encode("Three-1-2"));
        sender1.sendMessage(encode("blind-1-2"));
        sender1.sendMessage(encode("mices-1-2"));

        sender3.sendMessage(encode("Three-3-2"));
        sender3.sendMessage(encode("blind-3-2"));
        sender3.sendMessage(encode("mices-3-2"));
      }
      catch(final MessagingException e)
      {
        // oops, have to wait for the test to fail...
      }
    });

    final Thread thread3 = new Thread (() -> {
      try
      {
        sender1.sendMessage(encode("Three-1-3"));
        sender1.sendMessage(encode("blind-1-3"));
        sender1.sendMessage(encode("mices-1-3"));

        sender3.sendMessage(encode("Three-3-3"));
        sender3.sendMessage(encode("blind-3-3"));
        sender3.sendMessage(encode("mices-3-3"));
      }
      catch(final MessagingException e)
      {
        // oops, have to wait for the test to fail...
      }
    });

    thread1.start();
    thread2.start();
    thread3.start();

    thread1.join();
    thread2.join();
    thread3.join();

    waitForMessages();

    // can't really check message ordering due to asynchronicity (at least without a lot of complicated test code),
    // so just check the right messages are received regardless of order

    checkTopics(listener1, "a", 9);

    assertEquals(Arrays.asList("Three-1-1", "Three-1-2", "Three-1-3",
                               "blind-1-1", "blind-1-2", "blind-1-3",
                               "mices-1-1", "mices-1-2", "mices-1-3"),
                 listener1.messages.stream().map(MemoryMessagingTest::decode).sorted().collect(Collectors.toList()));

    assertEquals(Arrays.asList("a", "a", "a", "a", "a", "a", "a", "a", "a",  // count "xxxxx-1-x"
                               "b", "b", "b", "b", "b", "b", "b", "b", "b"), // count "xxxxx-2-x" and "xxxxx-3-x"
                 listener2.topics.stream().sorted().collect(Collectors.toList()));

    assertEquals(Arrays.asList("Three-1-1", "Three-1-2", "Three-1-3", "Three-2-2", "Three-3-2", "Three-3-3",
                               "blind-1-1", "blind-1-2", "blind-1-3", "blind-2-2", "blind-3-2", "blind-3-3",
                               "mices-1-1", "mices-1-2", "mices-1-3", "mices-2-2", "mices-3-2", "mices-3-3"),
                 listener2.messages.stream().map(MemoryMessagingTest::decode).sorted().collect(Collectors.toList()));
  }

  @Test
  public void messageIsolation() throws Exception
  {
    final MessageSender sender = startedSender("a");
    final StoringListener listener1 = new StoringListener();
    final StoringListener listener2 = new StoringListener();
    startedReceiver("a", listener1);
    startedReceiver("a", listener2);

    sender.sendMessage(encode("Hello"));
    waitForMessages();
    checkMessages(listener1, "Hello");
    checkMessages(listener2, "Hello");
    assertNotSame(listener1.messages.get(0), listener2.messages.get(0));
  }

  // -------------------------------------------------------------------------------- //

  private MessageSender startedSender(final String topic) throws MessagingException
  {
    final MessageSender sender = messagingFactory.createSender(topic);
    sender.start();
    return sender;
  }

  private StoringListener startedReceiver(final String topic) throws MessagingException
  {
    final StoringListener listener = new StoringListener();
    startedReceiver(topic, listener);
    return listener;
  }

  private MessageReceiver startedReceiver(final String topic, final MessageReceiveListener listener) throws MessagingException
  {
    final MessageReceiver receiver = messagingFactory.createReceiver(topic);
    receiver.setListener(listener);
    receiver.start();
    return receiver;
  }
  
  private MessageReceiver createReceiverNoListener(final String topic) throws MessagingException
  {
	  final MessageReceiver receiver = messagingFactory.createReceiver(topic);
	  
	  return receiver;
  }

  private void waitForMessages() throws Exception
  {
    //messagingFactory.waitForMessages(5000);
	  messagingFactory.waitForMessages();
  }
  

  private static void checkMessages(final StoringListener listener, final String... messages)
  {
    assertEquals(messages.length, listener.messages.size());
    for(int i=0; i<messages.length; ++i)
      assertEquals("Message " + i + " different", messages[i], decode(listener.messages.get(i)));
  }

  private static void checkTopics(final StoringListener listener, final String... topics)
  {
    assertEquals(topics.length, listener.topics.size());
    for(int i=0; i<topics.length; ++i)
      assertEquals("Topic " + i + " different", topics[i], listener.topics.get(i));
  }

  private static void checkTopics(final StoringListener listener, final String topic, final int num)
  {
    assertEquals(num, listener.topics.size());
    for(int i=0; i<num; ++i)
      assertEquals("Topic " + i + " different", topic, listener.topics.get(i));
  }

  private static byte[] encode(final String string)
  {
    return string.getBytes();
  }

  private static String decode(final Message message)
  {
    return new String(message.getMsg());
  }

  private static class StoringListener implements MessageReceiveListener
  {
    public final List<Message> messages = Collections.synchronizedList(new ArrayList<>());
    public final List<String> topics = Collections.synchronizedList(new ArrayList<>());


    @Override
    public void onMessage(final Message message, final String topic)
    {
    		messages.add(message);
    		topics.add(topic);
    }
  }
}
