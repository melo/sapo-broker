package pt.com.broker.messaging;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;

import pt.com.gcs.messaging.Gcs;

public class TopicSubscriberList
{
	// key: destinationName
	private static final Cache<String, TopicSubscriber> topicSubscribersCache = new Cache<String, TopicSubscriber>();

	private TopicSubscriberList()
	{
	}

	private static final CacheFiller<String, TopicSubscriber> topic_subscribers_cf = new CacheFiller<String, TopicSubscriber>()
	{
		public TopicSubscriber populate(String destinationName)
		{
			try
			{
				TopicSubscriber subscriber = new TopicSubscriber(destinationName);
				Gcs.addAsyncConsumer(destinationName, subscriber);
				return subscriber;
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
	};

	public static TopicSubscriber get(String destinationName)
	{
		try
		{
			return topicSubscribersCache.get(destinationName, topic_subscribers_cf);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}

	public static void removeValue(TopicSubscriber value)
	{
		try
		{
			topicSubscribersCache.removeValue(value);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}
}
