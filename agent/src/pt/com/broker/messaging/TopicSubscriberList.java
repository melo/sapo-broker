package pt.com.broker.messaging;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.messaging.Gcs;

public class TopicSubscriberList
{
	// key: destinationName
	private final Map<String, TopicSubscriber> topicSubscribersCache = new HashMap<String, TopicSubscriber>();
	
	private static final Logger log = LoggerFactory.getLogger(TopicSubscriberList.class);

	private static final TopicSubscriberList instance = new TopicSubscriberList();

	private TopicSubscriberList()
	{
	}

	private TopicSubscriber getSubscriber(String destinationName)
	{
		try
		{
			synchronized (topicSubscribersCache)
			{
				TopicSubscriber subscriber = null;

				if (topicSubscribersCache.containsKey(destinationName))
				{
					subscriber = topicSubscribersCache.get(destinationName);

					if (subscriber == null)
					{
						subscriber = createSubscriber(destinationName);
					}
				}
				else
				{
					subscriber = createSubscriber(destinationName);
				}

				return subscriber;
			}
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	private TopicSubscriber createSubscriber(String destinationName)
	{
		log.info("Create subscription for: '{}'", destinationName);
		TopicSubscriber subscriber = new TopicSubscriber(destinationName);
		Gcs.addAsyncConsumer(destinationName, subscriber);
		topicSubscribersCache.put(destinationName, subscriber);
		return subscriber;
	}

	private void removeSubscriber(String destinationName)
	{
		try
		{
			synchronized (topicSubscribersCache)
			{
				TopicSubscriber subscriber = topicSubscribersCache.remove(destinationName);
				Gcs.removeAsyncConsumer(subscriber);
			}
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	public static void remove(String destinationName)
	{
		log.info("Delete subscription for: '{}'", destinationName);
		instance.removeSubscriber(destinationName);
	}

	public static TopicSubscriber get(String destinationName)
	{
		return instance.getSubscriber(destinationName);
	}
}
