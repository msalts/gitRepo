package com.ph.coding.messaging.memory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.ph.coding.messaging.Message;
import com.ph.coding.messaging.MessagingException;

public class MemoryChannel {

	private final String topic;
	private final List<MemoryMessageReceiver> subscribedList = new CopyOnWriteArrayList<>();
	private volatile boolean isStarted = false;
	private final BlockingQueue<Message> queuedMessages= new LinkedBlockingQueue<>();
	private final Sender sender = new Sender();
	private Future<?> sendingFuture;
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	
	public MemoryChannel(String topic)
	{
		this.topic = topic;
	}
	
	public String getTopic()
	{
		return topic;
	}
	
	public void putMsg(Message msg) {
		if (isStarted) {
			try {
				queuedMessages.put(msg);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void subscribe(MemoryMessageReceiver receiver)
	{
		subscribedList.add(receiver);
		if(!isStarted)
		{
			isStarted = true;
			sendingFuture = executor.submit(sender);
		}
	}
	
	public void unsubscribe(MemoryMessageReceiver receiver)
	{
		subscribedList.remove(receiver);
		if(subscribedList.isEmpty())
		{
			isStarted = false;
			sendingFuture.cancel(true);
			executor.shutdown();
		}
	}
	
	//check if all messages have been sent
		public void checkAllMessagesSent()
		{
			synchronized(queuedMessages)
			{
				if(isStarted && !queuedMessages.isEmpty())
				{
					try {
						queuedMessages.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		}
		
	private final class Sender implements Callable<Boolean>{
		@Override
		public Boolean call() throws MessagingException {
			while (isStarted) {
				try {
						Message msg = queuedMessages.take();						
						for(MemoryMessageReceiver r: subscribedList)
						{
							Message cloneMessage = new MemoryMessage(msg.getMsg());
							r.addMessage(cloneMessage);
						}
						synchronized(queuedMessages)
						{
							if(queuedMessages.isEmpty())
							{
								queuedMessages.notify();
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
