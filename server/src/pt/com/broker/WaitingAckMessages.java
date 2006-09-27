package pt.com.broker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.JMSException;
import javax.jms.Message;

public class WaitingAckMessages
{
	private static final WaitingAckMessages instance = new WaitingAckMessages();

	private ConcurrentMap<String, Message> waitingAckMessages;

	private WaitingAckMessages()
	{
		waitingAckMessages = new ConcurrentHashMap<String, Message>();		
	}
	
	public static void put(String msgId, Message msg)
	{
		instance.waitingAckMessages.put(msgId, msg);
	}
	
	public static void ack(String msgId)
	{
		Message msg = instance.waitingAckMessages.remove(msgId);
		if (msg != null)
		{
			try
			{
				msg.acknowledge();
			}
			catch (JMSException e)
			{
				throw new RuntimeException(e);
			}
		}
		else
		{
			throw new RuntimeException("Non existing Message");
		}
	}

}
