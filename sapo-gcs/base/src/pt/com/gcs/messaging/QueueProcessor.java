package pt.com.gcs.messaging;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.io.DbStorage;

public class QueueProcessor
{
	private static Logger log = LoggerFactory.getLogger(QueueProcessor.class);

	private static final Random rnd = new Random();

	private final AtomicBoolean _isWorking = new AtomicBoolean(false);

	private final String _destinationName;

	private final AtomicLong _sequence = new AtomicLong(0L);

	private DbStorage dbStorage = new DbStorage();

	public QueueProcessor(String destinationName)
	{
		_destinationName = destinationName;
		// final Runnable waker = new Runnable()
		// {
		// public void run()
		// {
		// System.out.println("dbStorage.count: " +
		// dbStorage.count(_destinationName));
		// }
		// };
		// GcsExecutor.scheduleWithFixedDelay(waker, 10, 10, TimeUnit.SECONDS);
	}

	public void ack(Message msg)
	{
		log.debug("Ack message . AckId: {}", msg.getAckId());
		dbStorage.ackMessage(msg);
	}

	public final void wakeup()
	{
		log.debug("Wakeup Queue:enter");
		if (!hasRecipient())
		{
			log.warn("No recipient for this Destination");
			_isWorking.set(false);
			return;
		}

		if (_isWorking.getAndSet(true))
		{
			log.debug("Queue is already being processed.");
			return;
		}

		if (dbStorage.count(_destinationName) > 0)
		{
			if (hasRecipient())
			{
				log.debug("Processing stored messages");
				try
				{
					do
					{
						dbStorage.deliverStoredMessages(this, 0);
					}
					while ((dbStorage.count(_destinationName) > 0) && hasRecipient());
				}
				catch (Throwable t)
				{
					_isWorking.set(false);
					throw new RuntimeException(t);
				}
			}
			else
			{
				log.warn("No recipient for this Destination");
			}
		}
		_isWorking.set(false);
		log.debug("Wakeup Queue:exit");
	}

	public boolean deliverMessage(Message message)
	{
		message.setType((MessageType.COM_QUEUE));
		int lqsize = LocalQueueConsumers.size(_destinationName);
		int rqsize = RemoteQueueConsumers.size(_destinationName);
		// System.out.printf("lqsize: %s. rqsize: %s%n", lqsize, rqsize);

		if ((lqsize == 0) && (rqsize == 0))
		{
			return false;
		}
		else if ((lqsize == 0) && (rqsize != 0))
		{
			return RemoteQueueConsumers.notify(message);
		}
		else if ((rqsize == 0) && (lqsize != 0))
		{
			LocalQueueConsumers.notify(message);
		}
		else if ((lqsize > 0) && (rqsize > 0))
		{
			int n = rnd.nextInt() % 2;
			if (n == 0)
				return LocalQueueConsumers.notify(message);
			else
				return RemoteQueueConsumers.notify(message);
		}
		else
		{
			return false;
		}
		return false;
	}

	private boolean hasRecipient()
	{
		if (size() > 0)
			return true;
		else
			return false;
	}

	public void process(final Message msg)
	{
		try
		{
			if (!_isWorking.get())
			{
				if (hasRecipient())
				{
					if (deliverMessage(msg))
					{
						dbStorage.insert(msg, _sequence.incrementAndGet(), 1);
						return;
					}
				}
			}
			dbStorage.insert(msg, _sequence.incrementAndGet(), 0);
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	public long getQueuedMessages()
	{
		return dbStorage.count(_destinationName);
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
