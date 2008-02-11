package pt.com.gcs.messaging;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueueProcessor
{
	private static Logger log = LoggerFactory.getLogger(QueueProcessor.class);

	private final String _destinationName;

	private final AtomicLong _sequence = new AtomicLong(0L);
	
	private final AtomicBoolean isWorking = new AtomicBoolean(false);

	private final AtomicLong _deliverSequence = new AtomicLong(0L);

	private final ConcurrentMap<String, String> msgsAwaitingAck = new ConcurrentHashMap<String, String>();

	public QueueProcessor(String destinationName)
	{
		_destinationName = destinationName;
		log.info("Create Queue Processor for '{}'.", _destinationName);
	}

	public void ack(final String msgId)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Ack message . MsgId: '{}'.", msgId);
		}

		msgsAwaitingAck.remove(msgId);
		DbStorage.deleteMessage(msgId);

	}

	protected final void wakeup()
	{
		if (isWorking.getAndSet(true))
		{
			return;
		}
		
		if (getQueuedMessagesCount() > 0)
		{
			if (hasRecipient())
			{
				log.debug("Wakeup queue '{}'", _destinationName);
				try
				{
					DbStorage.recoverMessages(this);
				}
				catch (Throwable t)
				{
					throw new RuntimeException(t);
				}
			}
		}
		
		isWorking.set(false);
	}

	public boolean forward(Message message, boolean localConsumersOnly)
	{
		message.setType((MessageType.COM_QUEUE));
		int lqsize = LocalQueueConsumers.size(_destinationName);
		int rqsize = RemoteQueueConsumers.size(_destinationName);
		int size = lqsize + rqsize;

		boolean isDelivered = false;

		if (size == 0)
		{
			isDelivered = false;
		}
		else
		{
			if ((lqsize != 0) && localConsumersOnly)
			{
				isDelivered = LocalQueueConsumers.notify(message);
			}
			else if ((lqsize == 0) && (rqsize != 0))
			{
				isDelivered = RemoteQueueConsumers.notify(message);
			}
			else if ((rqsize == 0) && (lqsize != 0))
			{
				isDelivered = LocalQueueConsumers.notify(message);
			}
			else if ((lqsize > 0) && (rqsize > 0))
			{
				long n = _deliverSequence.incrementAndGet() % 2;
				if (n == 0)
					isDelivered = LocalQueueConsumers.notify(message);
				else
					isDelivered = RemoteQueueConsumers.notify(message);
			}
		}

		if (isDelivered)
		{
			putInAckWaitList(message.getMessageId());
		}

		return isDelivered;
	}

	public boolean hasRecipient()
	{
		if (size() > 0)
			return true;
		else
			return false;
	}

	public void store(final Message msg)
	{
		store(msg, false);
	}

	public void store(final Message msg, boolean localConsumersOnly)
	{
		try
		{
			long seq_nr = _sequence.incrementAndGet();
			DbStorage.insert(msg, seq_nr, 0, localConsumersOnly);
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	public void putInAckWaitList(String messageId)
	{
		msgsAwaitingAck.put(messageId, "");
	}

	public void removeFromAckWaitList(String messageId)
	{
		msgsAwaitingAck.remove(messageId);
	}

	public Set<String> getAckWaitList()
	{
		return msgsAwaitingAck.keySet();
	}

	public int ackWaitListSize()
	{
		return msgsAwaitingAck.size();
	}

	public long getQueuedMessagesCount()
	{
		return DbStorage.count(_destinationName);
	}

	public String getDestinationName()
	{
		return _destinationName;
	}

	public int size()
	{
		return RemoteQueueConsumers.size(_destinationName) + LocalQueueConsumers.size(_destinationName);
	}

}
