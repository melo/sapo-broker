package pt.com.broker.messaging;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Gcs;

public class QueueSessionListenerList
{

	private static final Logger log = LoggerFactory.getLogger(QueueSessionListenerList.class);

	// key: destinationName
	private static final Cache<String, QueueSessionListener> queueSessionListener = new Cache<String, QueueSessionListener>();

	private QueueSessionListenerList()
	{
	}

	private static final CacheFiller<String, QueueSessionListener> queue_listeners_cf = new CacheFiller<String, QueueSessionListener>()
	{
		public QueueSessionListener populate(String destinationName)
		{
			try
			{
				QueueSessionListener qsl = new QueueSessionListener();
				Gcs.addQueueConsumer(destinationName, qsl);
				return qsl;
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
	};

	public static QueueSessionListener get(String destinationName)
	{
		try
		{
			return queueSessionListener.get(destinationName, queue_listeners_cf);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}

	public static void removeValue(QueueSessionListener value)
	{
		try
		{
			queueSessionListener.removeValue(value);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
		}
	}
}
