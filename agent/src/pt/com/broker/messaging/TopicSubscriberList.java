package pt.com.broker.messaging;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Gcs;

public class TopicSubscriberList
{

	private static final Logger log = LoggerFactory.getLogger(TopicSubscriberList.class);

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
				TopicSubscriber subscriber = new TopicSubscriber();
				Gcs.addTopicConsumer(destinationName, subscriber);
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
