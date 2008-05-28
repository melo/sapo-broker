package pt.com.gcs.messaging;

import java.util.Collection;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueProcessorList
{

	private static final QueueProcessorList instance = new QueueProcessorList();

	private static final Logger log = LoggerFactory.getLogger(QueueProcessorList.class);

	private static final CacheFiller<String, QueueProcessor> qp_cf = new CacheFiller<String, QueueProcessor>()
	{
		public QueueProcessor populate(String destinationName)
		{
			try
			{
				log.debug("Populate QueueProcessorList");
				QueueProcessor qp = new QueueProcessor(destinationName);
				return qp;
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
	};

	public static QueueProcessor get(String destinationName)
	{
		return instance.i_get(destinationName);
	}

	public static void remove(String queueName)
	{
		instance.i_remove(queueName);
	}

	public static void removeValue(QueueProcessor value)
	{
		instance.i_removeValue(value);
	}

	public static int size()
	{
		return instance.i_size();
	}

	public static int size(String destinationName)
	{
		return get(destinationName).size();
	}

	public static Collection<QueueProcessor> values()
	{
		return instance.i_values();
	}

	// key: destinationName
	private Cache<String, QueueProcessor> qpCache = new Cache<String, QueueProcessor>();

	private QueueProcessorList()
	{
	}

	private QueueProcessor i_get(String destinationName)
	{
		log.debug("Get Queue for: {}", destinationName);

		try
		{
			return qpCache.get(destinationName, qp_cf);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}

	private void i_remove(String queueName)
	{
		try
		{
			synchronized (qpCache)
			{
				QueueProcessor qp = get(queueName);
				qpCache.remove(queueName);
				LocalQueueConsumers.delete(queueName);
				RemoteQueueConsumers.delete(queueName);
				qp.clearStorage();
				DispatcherList.removeDispatcher(queueName);
				log.info("Destination '{}' was deleted", queueName);
			}
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}

	private void i_removeValue(QueueProcessor value)
	{
		try
		{
			qpCache.removeValue(value);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}

	private int i_size()
	{
		return qpCache.size();
	}

	private Collection<QueueProcessor> i_values()
	{
		try
		{
			return qpCache.values();
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}
}
