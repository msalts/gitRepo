package com.ph.coding.messaging.memory;

import com.ph.coding.messaging.Message;

public class MemoryMessage implements Message {

	private final byte[] msg;
	
	public MemoryMessage(byte[] msg)
	{
		this.msg = msg;
	}
	@Override
	public byte[] getMsg() {
		return msg;
	}

	@Override
	public void dispose() {
		// dispose of message

	}

}
