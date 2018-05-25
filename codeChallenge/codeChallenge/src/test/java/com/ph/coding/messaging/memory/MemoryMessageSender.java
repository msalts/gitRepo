package com.ph.coding.messaging.memory;

import com.ph.coding.messaging.Message;
import com.ph.coding.messaging.MessageSender;
import com.ph.coding.messaging.MessagingException;

public final class MemoryMessageSender implements MessageSender {

	private volatile boolean isStarted = false;
	private final String topic;
	private final MemoryChannel channel;
	
	public MemoryMessageSender(MemoryChannel channel)
	{
		this.channel  = channel;
		this.topic = channel.getTopic();
	}
	
	
	
	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public boolean isStarted() {
		return isStarted;
	}

	@Override
	public void start() throws MessagingException {
		if(channel == null)
		{
			throw new MessagingException("No channel");
		}
		isStarted = true;

	}

	@Override
	public void stop() throws MessagingException {
		isStarted = false;
	}

	@Override
	public void sendMessage(byte[] message) throws MessagingException {
		if(!isStarted)
		{
			throw new MessagingException("Not started");
		}
		Message msg = new MemoryMessage(message);
		channel.putMsg(msg);
	}

}
