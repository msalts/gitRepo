package com.ph.coding.messaging;

public class MessageReceiverImpl implements MessageReceiver {

	private MessageReceiveListener listener;
	private final String topic;
	private boolean isStarted = false;
	
	MessageReceiverImpl(String topic)
	{
		this.topic = topic;
	}
	
	@Override
	public String getTopic() {
		// TODO Auto-generated method stub
		return topic;
	}

	@Override
	public boolean isStarted() {
		return isStarted;
	}

	@Override
	public void start() throws MessagingException {

		isStarted = true;
		
	}

	@Override
	public void stop() throws MessagingException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setListener(MessageReceiveListener listener) {
		this.listener = listener;
	}

}
