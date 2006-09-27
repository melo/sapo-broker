package pt.com.broker;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.ds.Cache;
import pt.com.ds.CacheFiller;

public class SessionTopicConsumerList
{
	private static final Logger log = LoggerFactory.getLogger(SessionTopicConsumerList.class);

	// key: destinationName
	private static final Cache<String, List<SessionConsumer>> sessionsConsumerCache = new Cache<String, List<SessionConsumer>>();

	private SessionTopicConsumerList()
	{
	}

	private static final CacheFiller<String, List<SessionConsumer>> session_consumers_cf = new CacheFiller<String, List<SessionConsumer>>()
	{
		public List<SessionConsumer> populate(String destinationName)
		{
			try
			{
				log.info("Create Session Consumer list for : " + destinationName);

				return new ArrayList<SessionConsumer>();
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
	};

	public static List<SessionConsumer> get(String destinationName)
	{
		try
		{
			return sessionsConsumerCache.get(destinationName, session_consumers_cf);
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
			sessionsConsumerCache.remove(destinationName);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
		}
	}

	public static int getListSize(String destinationName)
	{
		List<SessionConsumer> l = get(destinationName);
		synchronized (l)
		{
			return l.size();
		}
	}
}
