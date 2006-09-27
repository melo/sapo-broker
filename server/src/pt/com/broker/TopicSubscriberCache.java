package pt.com.broker;

import javax.jms.MessageConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.ds.Cache;
import pt.com.ds.CacheFiller;

public class TopicSubscriberCache
{
	private static final BrokerConsumer _brokerConsumer = BrokerConsumer.getInstance();

	private static final Logger log = LoggerFactory.getLogger(TopicSubscriberCache.class);

	// key: destinationName
	private static final Cache<String, TopicSubscriber> topicSubscribersCache = new Cache<String, TopicSubscriber>();

	private TopicSubscriberCache()
	{
	}

	private static final CacheFiller<String, TopicSubscriber> topic_subscribers_cf = new CacheFiller<String, TopicSubscriber>()
	{
		public TopicSubscriber populate(String destinationName)
		{
			try
			{
				MessageConsumer consumer = _brokerConsumer.getMessageConsumer(AcknowledgeMode.AUTO, "TOPIC", destinationName);
				TopicSubscriber subscriber = new TopicSubscriber(consumer, destinationName);
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
		}
	}
}
