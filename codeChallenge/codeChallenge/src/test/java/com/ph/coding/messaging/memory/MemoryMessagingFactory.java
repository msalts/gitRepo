package com.ph.coding.messaging.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ph.coding.messaging.MessageReceiver;
import com.ph.coding.messaging.MessageSender;
import com.ph.coding.messaging.MessagingException;
import com.ph.coding.messaging.MessagingFactory;

public class MemoryMessagingFactory implements MessagingFactory
{
	private final Map<String, MemoryChannel> channels = new ConcurrentHashMap<>();
	private final List<MemoryMessageSender> senders = Collections.synchronizedList(new ArrayList<>());
	private final List<MemoryMessageReceiver> receivers = Collections.synchronizedList(new ArrayList<>()); 
	private volatile boolean isStarted;
  @Override
  public String getProviderName()
  {
    return "InMemory";
  }

  @Override
  public void start() throws Exception
  {
    isStarted = true;
  }

  @Override
	public void shutdown() throws Exception {
	  isStarted = false;
		synchronized (senders) {
			Iterator<MemoryMessageSender> itSend = senders.iterator();
			while (itSend.hasNext()) {
				MessageSender sender = itSend.next();
				sender.stop();
				itSend.remove();
			}
		}
		synchronized(receivers)
		{
			Iterator<MemoryMessageReceiver> itRec = receivers.iterator();
			while(itRec.hasNext())
			{
				MessageReceiver receiver = itRec.next();
				receiver.stop();
				itRec.remove();
			}
		}
		channels.clear();
	}

  @Override
  public MessageSender createSender(final String topic) throws MessagingException
  {
	  if(!isStarted) throw new MessagingException("Not started factory");
	MemoryChannel mc = channels.computeIfAbsent(topic, chan -> new MemoryChannel(topic));
	 if(mc == null)
	 {
		 mc = channels.get(topic);
	 }
    MemoryMessageSender sender = new MemoryMessageSender(mc);
    senders.add(sender);
    return sender;
  }

  @Override
  public MessageReceiver createReceiver(final String topic) throws MessagingException
  {
	  if(!isStarted) throw new MessagingException("Not started factory");
	 MemoryChannel mc = channels.computeIfAbsent(topic, chan -> new MemoryChannel(topic));
	 if(mc == null)
	 {
		 mc = channels.get(topic);
	 }
    MemoryMessageReceiver receiver = new MemoryMessageReceiver(topic, mc);
    receivers.add(receiver);
    return receiver;
  }
  

  public void waitForMessages(final long timeoutMillis) throws Exception
  {
    //TODO - a proper wait/notify mechanism is needed really, but this will probably work for now...
    Thread.sleep(Math.min(500, timeoutMillis));
  }
  
  public void waitForMessages()
  {
	  synchronized(channels)
	  {
		  for(MemoryChannel channel : channels.values())
		  {
			  channel.checkAllMessagesSent();
		  }
	  }
	  synchronized(receivers)
	  {
		  for(MemoryMessageReceiver receiver : receivers)
		  {
			  receiver.checkQueueIsEmpty();
		  }
	  }
  }

public boolean isStarted() {
	return isStarted;
}
  
}
