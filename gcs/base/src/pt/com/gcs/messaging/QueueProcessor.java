package pt.com.gcs.messaging;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.caudexorigo.concurrent.Sleep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.io.DbStorage;
import pt.com.gcs.tasks.GcsExecutor;

public class QueueProcessor
{
	private static Logger log = LoggerFactory.getLogger(QueueProcessor.class);

	private static final Random rnd = new Random();

	private final AtomicBoolean _isWorking = new AtomicBoolean(false);

	private final String _destinationName;

	private final AtomicLong _sequence = new AtomicLong(0L);

	public QueueProcessor(String destinationName)
	{
		_destinationName = destinationName;
		log.info("Create Queue Processor for '{}'.", _destinationName);
	}

	public static void ack(final String msgId)
	{
// final Runnable wack = new Runnable()
// {
// public void run()
// {
// if (log.isDebugEnabled())
// {
// log.debug("Ack message . MsgId: '{}'.", msgId);
// }
// DbStorage.ackMessage(msgId);
// }
// };
// GcsExecutor.execute(wack);
		if (log.isDebugEnabled())
		{
			log.debug("Ack message . MsgId: '{}'.", msgId);
		}
		DbStorage.ackMessage(msgId);
	}

	public final void wakeup()
	{
		if (!hasRecipient())
		{
			log.warn("No recipient for this Destination.");
			_isWorking.set(false);
			return;
		}

		if (size() >= 2)
		{
			log.info("There are at least one active consumers for this queue.");
			_isWorking.set(false);
			return;
		}

		if (_isWorking.getAndSet(true))
		{
			log.debug("Queue is already being processed.");
			return;
		}

		if (DbStorage.count(_destinationName) > 0)
		{
			if (hasRecipient())
			{
				log.debug("Processing stored messages.");
				try
				{
					Runnable dbcounter = new Runnable()
					{
						public void run()
						{
							long cnt = DbStorage.count(_destinationName);
							while (cnt > 0)
							{
								log.info("Queue '{}' has {} message(s) to recover.", _destinationName, cnt);
								Sleep.time(5000);
								cnt = DbStorage.count(_destinationName);
							}
							log.info("Queue '{}' recovery is complete.", _destinationName);
						}
					};
					GcsExecutor.execute(dbcounter);

					do
					{
						DbStorage.recoverMessages(this);
					}
					while ((DbStorage.count(_destinationName) > 0) && hasRecipient());

				}
				catch (Throwable t)
				{
					_isWorking.set(false);
					throw new RuntimeException(t);
				}
			}
			else
			{
				log.warn("No recipient for this Destination.");
			}
		}
		_isWorking.set(false);
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
			return LocalQueueConsumers.notify(message);
		}
		else if ((lqsize > 0) && (rqsize > 0))
		{
			int n = rnd.nextInt() % 2;
			if (n == 0)
				return LocalQueueConsumers.notify(message);
			else
				return RemoteQueueConsumers.notify(message);
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
					DbStorage.insert(msg, _sequence.incrementAndGet(), 1);
					if (!deliverMessage(msg))
					{
						DbStorage.deleteMessage(msg.getMessageId());
						DbStorage.insert(msg, _sequence.incrementAndGet(), 0);
						return;
					}
					return;
				}
			}
			DbStorage.insert(msg, _sequence.incrementAndGet(), 0);
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	public long getQueuedMessages()
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
