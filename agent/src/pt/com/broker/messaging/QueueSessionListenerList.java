package pt.com.broker.messaging;

import java.util.Collection;

import org.apache.mina.common.IoSession;
import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;

import pt.com.gcs.messaging.Gcs;

public class QueueSessionListenerList
{
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
				QueueSessionListener qsl = new QueueSessionListener(destinationName);
				Gcs.addAsyncConsumer(destinationName, qsl);
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

	public static void removeSession(IoSession iosession)
	{
		try
		{
			Collection<QueueSessionListener> list = queueSessionListener.values();
			for (QueueSessionListener queueSessionListener : list)
			{
				queueSessionListener.removeConsumer(iosession);
			}

		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
		}
	}
}
