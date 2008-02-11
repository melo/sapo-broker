package pt.com.gcs.messaging;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceivedMessagesBufferList
{

	private static final Logger log = LoggerFactory.getLogger(ReceivedMessagesBufferList.class);

	// key: destinationName
	private static final Cache<String, ReceivedMessagesBuffer> qpCache = new Cache<String, ReceivedMessagesBuffer>();

	private ReceivedMessagesBufferList()
	{
	}

	private static final CacheFiller<String, ReceivedMessagesBuffer> qp_cf = new CacheFiller<String, ReceivedMessagesBuffer>()
	{
		public ReceivedMessagesBuffer populate(String destinationName)
		{
			try
			{
				log.debug("Populate ReceivedMessagesBufferList");
				
				ReceivedMessagesBuffer qp = new ReceivedMessagesBuffer();
				return qp;
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
	};

	public static ReceivedMessagesBuffer get(String destinationName)
	{
		log.debug("Get ReceivedMessagesBuffer for: {}", destinationName);
		
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

	public static void removeValue(ReceivedMessagesBuffer value)
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
