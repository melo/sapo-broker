package pt.com.broker.client;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;

public class SyncConsumerList
{
	// key: destinationName
	private static final Cache<String, SyncConsumer> bq_list = new Cache<String, SyncConsumer>();

	private SyncConsumerList()
	{
	}

	private static final CacheFiller<String, SyncConsumer> bq_cf = new CacheFiller<String, SyncConsumer>()
	{
		public SyncConsumer populate(String destinationName)
		{
			try
			{
				SyncConsumer bq = new SyncConsumer();
				return bq;
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
	};

	public static SyncConsumer get(String destinationName)
	{
		try
		{
			return bq_list.get(destinationName, bq_cf);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}

	public static void remove(String destinationName)
	{
		try
		{
			bq_list.remove(destinationName);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
		}
	}
}
