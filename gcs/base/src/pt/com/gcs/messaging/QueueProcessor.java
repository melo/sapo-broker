package pt.com.gcs.messaging;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.mina.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueProcessor
{
	private static Logger log = LoggerFactory.getLogger(QueueProcessor.class);

	private final String _destinationName;

	private final AtomicLong _sequence = new AtomicLong(0L);

	private final AtomicBoolean isWorking = new AtomicBoolean(false);

	private final AtomicLong _deliverSequence = new AtomicLong(0L);

	private final AtomicLong _counter;

	private final AtomicInteger _skippedWakeups = new AtomicInteger(0);

	protected final AtomicBoolean emptyQueueInfoDisplay = new AtomicBoolean(false);

	private final Set<String> msgsAwaitingAck = new ConcurrentHashSet<String>();

	private final Set<String> reservedMessages = new ConcurrentHashSet<String>();

	protected QueueProcessor(String destinationName)
	{
		_destinationName = destinationName;
		_counter = new AtomicLong(DbStorage.count(destinationName));
		reservedMessages.add(UUID.randomUUID().toString());
		log.info("Create Queue Processor for '{}'.", _destinationName);
		log.info("Queue '{}' has {} message(s).", destinationName, _counter.get());
	}

	protected void ack(final String msgId)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Ack message . MsgId: '{}'.", msgId);
		}

		if (DbStorage.deleteMessage(msgId, _destinationName))
		{
			decrementMsgCounter();
		}

		msgsAwaitingAck.remove(msgId);
		reservedMessages.remove(msgId);

	}

	protected final void wakeup()
	{
		if (isWorking.getAndSet(true))
		{
			return;
		}
		long cnt = getQueuedMessagesCount();
		if (cnt > 0)
		{
			emptyQueueInfoDisplay.set(false);

			if (hasRecipient())
			{
				log.debug("Wakeup queue '{}'", _destinationName);
				try
				{
					int ackWaitListSize = ackWaitListSize();

					if (ackWaitListSize > 0)
					{
						if (_skippedWakeups.incrementAndGet() < 10)
						{
							log.warn("Processing for queue '{}' was skipped. There are '{}' message(s) waiting for ACK.", _destinationName, ackWaitListSize);
							isWorking.set(false);
							return;
						}
						else
						{
							log.error("The maximum amount of skipped wakeups was reached. There are '{}' message(s) waiting for ACK, but the processing for queue '{}' must proceed.", ackWaitListSize, _destinationName);
							msgsAwaitingAck.clear();
						}
					}
					_skippedWakeups.set(0);
					DbStorage.recoverMessages(this);
				}
				catch (Throwable t)
				{
					throw new RuntimeException(t);
				}
			}
		}
		else if (cnt < 0)
		{
			log.warn("Queue '{}' as an invalid message count: {}. Will try to fix", getDestinationName(), cnt);

			synchronized (_counter)
			{
				_counter.set(DbStorage.count(getDestinationName()));
			}
			log.info("Queue '{}' has {} message(s).", getDestinationName(), cnt);
		}
		isWorking.set(false);
	}

	protected boolean forward(Message message, boolean localConsumersOnly)
	{
		message.setType((MessageType.COM_QUEUE));
		int lqsize = LocalQueueConsumers.size(_destinationName);
		int rqsize = RemoteQueueConsumers.size(_destinationName);
		int size = lqsize + rqsize;

		boolean isDelivered = false;
		putInAckWaitList(message.getMessageId());

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

		if (!isDelivered)
		{
			msgsAwaitingAck.remove(message.getMessageId());
		}
		return isDelivered;
	}

	protected boolean hasRecipient()
	{
		if (size() > 0)
			return true;
		else
			return false;
	}

	protected void store(final Message msg)
	{
		store(msg, false);
	}

	protected void store(final Message msg, boolean localConsumersOnly)
	{
		try
		{
			long seq_nr = _sequence.incrementAndGet();
			DbStorage.insert(msg, seq_nr, 0, localConsumersOnly);
			incrementMsgCounter();
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	protected void putInAckWaitList(String messageId)
	{
		msgsAwaitingAck.add(messageId);
	}

	protected Set<String> getAckWaitList()
	{
		return msgsAwaitingAck;
	}

	protected Set<String> getReservedMessages()
	{
		return reservedMessages;
	}

	protected void removeFromReservedMessages(String messageId)
	{
		reservedMessages.remove(messageId);
	}

	protected int ackWaitListSize()
	{
		return msgsAwaitingAck.size();
	}

	public long getQueuedMessagesCount()
	{
		return _counter.get();
	}

	protected void incrementMsgCounter()
	{
		_counter.incrementAndGet();
	}

	protected void decrementMsgCounter()
	{
		_counter.decrementAndGet();
	}

	protected String getDestinationName()
	{
		return _destinationName;
	}

	protected int size()
	{
		return RemoteQueueConsumers.size(_destinationName) + LocalQueueConsumers.size(_destinationName);
	}

	protected void incrementDeliveryCount(String msg_id)
	{
		DbStorage.incrementDeliveryCount(msg_id, _destinationName);
	}

	protected Message poll()
	{
		int lqsize = LocalQueueConsumers.size(_destinationName);
		if (lqsize > 0)
		{
			throw new IllegalStateException("An async consumer already exists, it's not possible to mix sync and async consumers for the same Queue on the same peer.");
		}
		return DbStorage.poll(this);
	}

}
