package pt.com.broker;

import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import pt.com.ds.Cache;
import pt.com.ds.CacheFiller;
import pt.com.text.StringUtils;

public class MessageProducerCache
{
	// key: destinationName
	private static final Cache<String, MessageProducer> _producers = new Cache<String, MessageProducer>();

	private MessageProducerCache()
	{
	}

	public static MessageProducer get(final String prod_key, final Session jms_session)
	{
		final CacheFiller<String, MessageProducer> producer_cf = new CacheFiller<String, MessageProducer>()
		{
			public MessageProducer populate(String key)
			{
				MessageProducer producer;
				try
				{
					String[] params = StringUtils.split(key, '$');
					String destinationType = params[0];
					String destinationName = params[1];

					if (destinationType.equals("TOPIC"))
					{
						Topic topic = BrokerDestination.getTopic(destinationName, jms_session);
						producer = jms_session.createProducer(topic);
					}
					else if (destinationType.equals("QUEUE"))
					{
						Queue queue = BrokerDestination.getQueue(destinationName, jms_session);
						producer = jms_session.createProducer(queue);
					}
					else
					{
						throw new IllegalArgumentException("Not a valid destination type!");
					}

					return producer;
				}
				catch (Throwable e)
				{
					throw new RuntimeException(e);
				}
			}
		};

		try
		{
			return _producers.get(prod_key, producer_cf);
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
	}

	public static int size()
	{
		return _producers.size();
	}
}
