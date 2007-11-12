package pt.com.gcs.messaging;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueProcessorList
{

	private static final Logger log = LoggerFactory.getLogger(QueueProcessorList.class);

	// key: destinationName
	private static final Cache<String, QueueProcessor> qpCache = new Cache<String, QueueProcessor>();

	private QueueProcessorList()
	{
	}

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

	public static void removeValue(QueueProcessor value)
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
	
	public static int size()
	{
		return qpCache.size();
	}
}
