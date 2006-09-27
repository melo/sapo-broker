package pt.com.broker;

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.ds.Cache;
import pt.com.ds.CacheFiller;
import pt.com.text.StringUtils;

public class BrokerDestination
{
	private static final Logger log = LoggerFactory.getLogger(BrokerDestination.class);

	private static final BrokerDestination instance = new BrokerDestination();

	private final Cache<String, Topic> topics;

	private final Cache<String, Queue> queues;

	private static final char[] ics = { '*', '#' };

	private BrokerDestination()
	{
		topics = new Cache<String, Topic>();
		queues = new Cache<String, Queue>();
	}

	public static Topic getTopic(final String topicName, final Session jmsSession)
	{
		if (StringUtils.containsNone(topicName, ics))
		{
			final CacheFiller<String, Topic> topic_cf = new CacheFiller<String, Topic>()
			{
				public Topic populate(String key)
				{
					try
					{
						log.info("Create Topic: " + topicName);
						return jmsSession.createTopic(topicName);
					}
					catch (Throwable e)
					{
						throw new RuntimeException(e);
					}
				}
			};

			try
			{
				return instance.topics.get(topicName, topic_cf);
			}
			catch (Throwable e)
			{
				if (e instanceof InterruptedException)
				{
					Thread.currentThread().interrupt();				
				}
				throw new RuntimeException(e);
			}
		}
		else
		{
			throw new IllegalArgumentException("Invalid characters in Topic name");
		}

	}

	public static Queue getQueue(final String queueName, final Session jmsSession)
	{
		if (StringUtils.containsNone(queueName, ics))
		{
			final CacheFiller<String, Queue> topic_cf = new CacheFiller<String, Queue>()
			{
				public Queue populate(String key)
				{
					try
					{
						log.info("Create Queue: " + queueName);
						return jmsSession.createQueue(queueName);
					}
					catch (Throwable e)
					{
						throw new RuntimeException(e);
					}
				}
			};

			try
			{
				return instance.queues.get(queueName, topic_cf);
			}
			catch (Throwable e)
			{
				if (e instanceof InterruptedException)
				{
					Thread.currentThread().interrupt();				
				}
				throw new RuntimeException(e);
			}
		}
		else
		{
			throw new IllegalArgumentException("Invalid characters in Queue name");
		}
	}
}
