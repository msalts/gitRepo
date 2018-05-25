package com.ph.coding.messaging.memory;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.ph.coding.messaging.Message;
import com.ph.coding.messaging.MessageReceiveListener;
import com.ph.coding.messaging.MessageReceiver;
import com.ph.coding.messaging.MessagingException;

public final class MemoryMessageReceiver implements MessageReceiver {

	private final String topic;
	private volatile boolean isStarted = false;
	private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
	private volatile MessageReceiveListener listener;
	private final MemoryChannel channel;
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private final Poller poller = new Poller();
	private Future<?> pollingFuture;
	
	public MemoryMessageReceiver(String topic, MemoryChannel channel)
	{
		this.topic = topic;
		this.channel = channel;
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
		if(listener == null)
		{
			throw new MessagingException("No listener");
		}
		channel.subscribe(this);	
		isStarted = true;
		pollingFuture = executor.submit(poller);
	}

	@Override
	public void stop() throws MessagingException {
		isStarted = false;
		pollingFuture.cancel(true);
		executor.shutdown();
		channel.unsubscribe(this);
	}

	@Override
	public void setListener(MessageReceiveListener listener) {
			this.listener = listener;
	}
	
	//Attempting to see if all messages have been processed
	public void checkQueueIsEmpty()
	{
		synchronized(queue)
		{
			if(!queue.isEmpty())
			{
				try {
					queue.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void addMessage(Message msg) {
		try {
			queue.put(msg);
		} catch (InterruptedException e) {
			System.out.println("Interrupted! " + e.getMessage());
		}
	}
	
	private class Poller implements Callable<Boolean> {


		@Override
		public Boolean call() throws MessagingException {
			while (isStarted) {
				try {
						Message msg = queue.take();

							if(listener == null) throw new MessagingException("No listener whilst trying to process message");
							listener.onMessage(msg, topic);
						synchronized(queue)
						{
							if(queue.isEmpty())
							{
								queue.notify();
							}
						}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
		}
			return isStarted;
		}
		
	}
}
