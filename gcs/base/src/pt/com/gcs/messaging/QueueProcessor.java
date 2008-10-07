package pt.com.gcs.messaging;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueProcessor
{
	private static Logger log = LoggerFactory.getLogger(QueueProcessor.class);

	private final String _destinationName;

	private final AtomicLong _sequence;

	private final AtomicBoolean isWorking = new AtomicBoolean(false);

	private final AtomicLong _deliverSequence = new AtomicLong(0L);

	protected final AtomicBoolean emptyQueueInfoDisplay = new AtomicBoolean(false);

	private final BDBStorage storage;

	protected QueueProcessor(String destinationName)
	{
		if (StringUtils.isBlank(destinationName))
		{
			throw new IllegalArgumentException(String.format("'%s' is not a valid Queue name", destinationName));
		}

		_destinationName = destinationName;

		storage = new BDBStorage(this);

		if (storage.count() == 0)
		{
			_sequence = new AtomicLong(0L);
		}
		else
		{
			_sequence = new AtomicLong(storage.getLastSequenceValue());
		}

		if (StringUtils.contains(destinationName, "@"))
		{
			DispatcherList.create(destinationName);
		}

		log.info("Create Queue Processor for '{}'.", _destinationName);
		log.info("Queue '{}' has {} message(s).", destinationName, getQueuedMessagesCount());
	}

	protected void ack(final String msgId)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Ack message . MsgId: '{}'.", msgId);
		}

		storage.deleteMessage(msgId);
	}

	protected final void wakeup()
	{
		if (isWorking.getAndSet(true))
		{
			log.debug("Queue '{}' is running, skip wakeup", _destinationName);
			return;
		}
		long cnt = getQueuedMessagesCount();
		if (cnt > 0)
		{
			emptyQueueInfoDisplay.set(false);

			if (hasRecipient())
			{
				try
				{
					log.debug("Wakeup queue '{}'", _destinationName);
					storage.recoverMessages();
				}
				catch (Throwable t)
				{
					log.error(t.getMessage(), t);
					throw new RuntimeException(t);
				}
				finally
				{
					isWorking.set(false);
				}
			}
			else
			{
				log.debug("Queue '{}' does not have asynchronous consumers", _destinationName);
			}
		}
		isWorking.set(false);
	}

	protected boolean forward(Message message, boolean preferLocalConsumer) throws IllegalStateException
	{
		message.setType((MessageType.COM_QUEUE));
		int lqsize = LocalQueueConsumers.size(_destinationName);
		int rqsize = RemoteQueueConsumers.size(_destinationName);
		int size = lqsize + rqsize;

		boolean isDelivered = false;

		if (size == 0)
		{
			throw new IllegalStateException("There are no consumers.");
		}
		else
		{
			if ((lqsize != 0) && preferLocalConsumer)
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

		if (log.isDebugEnabled())
		{
			log.debug("forward-> isDelivered: " + isDelivered + ", lqsize: " + lqsize + ", rqsize: " + rqsize + ", message.id: " + message.getMessageId());
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
			storage.insert(msg, seq_nr, 0, localConsumersOnly);
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	public long getQueuedMessagesCount()
	{
		return storage.count();
	}

	protected String getDestinationName()
	{
		return _destinationName;
	}

	protected int size()
	{
		return RemoteQueueConsumers.size(_destinationName) + LocalQueueConsumers.size(_destinationName);
	}

	protected Message poll()
	{
		return storage.poll();
	}

	public synchronized void clearStorage()
	{
		storage.deleteQueue();
	}

}
