package pt.com.broker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.Message;
import javax.jms.MessageProducer;

import pt.com.text.StringUtils;

public class TopicToQueueDispatcher
{
	private static final TopicToQueueDispatcher instance = new TopicToQueueDispatcher();

	private final ConcurrentMap<String, ConcurrentMap<String, MessageProducer>> dispatchers;

	private TopicToQueueDispatcher()
	{
		dispatchers = new ConcurrentHashMap<String, ConcurrentMap<String, MessageProducer>>();
	}

	public static synchronized void add(Notify sb)
	{
		String sourceTopic = extractTopicName(sb.destinationName);
		ConcurrentMap<String, MessageProducer> queueMapper = instance.dispatchers.get(sourceTopic);

		if (queueMapper == null)
		{
			queueMapper = new ConcurrentHashMap<String, MessageProducer>();
			instance.dispatchers.put(sourceTopic, queueMapper);
		}

		if (queueMapper.get(sb.destinationName) == null)
		{
			MessageProducer producer = BrokerProducer.getInstance().getMessageProducer("QUEUE", sb.destinationName);
			queueMapper.put(sb.destinationName, producer);
		}
	}

	public static void forward(String sourceTopicName, Message msg)
	{
		if (instance.dispatchers.size() > 0)
		{
			ConcurrentMap<String, MessageProducer> queueMapper = instance.dispatchers.get(sourceTopicName);
			if (queueMapper != null)
			{
				for (MessageProducer producer : queueMapper.values())
				{
					try
					{
						producer.send(msg);
					}
					catch (Throwable e)
					{
						throw new RuntimeException(e);
					}
				}
			}
		}
	}

	public static void validateDestinationName(String destinationName)
	{
		int c_markers = StringUtils.countMatches(destinationName, "@");
		if (c_markers != 1)
		{
			throw new IllegalArgumentException("must have one and only one '@' separator");
		}
	}

	private static String extractTopicName(String destinationName)
	{
		validateDestinationName(destinationName);
		int marker = destinationName.indexOf("@");

		return destinationName.substring(marker + 1);
	}
}
