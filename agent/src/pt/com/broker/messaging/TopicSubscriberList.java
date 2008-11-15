package pt.com.broker.messaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.BrokerExecutor;
import pt.com.gcs.conf.GcsInfo;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;

public class TopicSubscriberList
{
	// key: destinationName
	private static final Map<String, TopicSubscriber> topicSubscribersCache = new HashMap<String, TopicSubscriber>();

	private static final Logger log = LoggerFactory.getLogger(TopicSubscriberList.class);

	private static final TopicSubscriberList instance = new TopicSubscriberList();

	private TopicSubscriberList()
	{
		Runnable counter = new Runnable()
		{
			public void run()
			{
				synchronized (topicSubscribersCache)
				{
					try
					{
						Collection<TopicSubscriber> subs = topicSubscribersCache.values();

						for (TopicSubscriber topicSubscriber : subs)
						{
							int ssize = topicSubscriber.count();

							Message cnt_message = new Message();
							String ctName = String.format("/system/stats/topic-consumer-count/#%s#", topicSubscriber.getDestinationName());
							String content = GcsInfo.getAgentName() + "#" + topicSubscriber.getDestinationName() + "#" + ssize;
							cnt_message.setDestination(ctName);
							cnt_message.setContent(content);
							Gcs.publish(cnt_message);
						}
					}
					catch (Throwable e)
					{
						log.error(e.getMessage(), e);
					}
				}
			}
		};

		BrokerExecutor.scheduleWithFixedDelay(counter, 20, 20, TimeUnit.SECONDS);
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

	private void i_removeSubscriber(String destinationName)
	{
		try
		{
			synchronized (topicSubscribersCache)
			{
				TopicSubscriber subscriber = topicSubscribersCache.remove(destinationName);
				if (subscriber != null)
				{
					Gcs.removeAsyncConsumer(subscriber);
				}
			}

		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	public static void removeSession(IoSession iosession)
	{
		synchronized (topicSubscribersCache)
		{
			try
			{
				Collection<TopicSubscriber> list = topicSubscribersCache.values();
				List<TopicSubscriber> toDelete = new ArrayList<TopicSubscriber>();

				for (TopicSubscriber subscriber : list)
				{
					int scount = subscriber.removeSessionConsumer(iosession);
					if (scount == 0)
					{
						toDelete.add(subscriber);
					}
				}

				for (TopicSubscriber tsubs : toDelete)
				{
					topicSubscribersCache.remove(tsubs.getDestinationName());
					Gcs.removeAsyncConsumer(tsubs);
				}

				toDelete.clear();
			}
			catch (Throwable t)
			{
				log.error(t.getMessage(), t);
			}
		}
	}

	public static void removeSubscriber(String destinationName)
	{
		log.info("Delete subscription for: '{}'", destinationName);
		instance.i_removeSubscriber(destinationName);
	}

	public static TopicSubscriber get(String destinationName)
	{
		return instance.getSubscriber(destinationName);
	}
}
