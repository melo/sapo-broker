package pt.com.broker;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.text.StringUtils;

public class TopicAsQueueListener implements MessageListener
{
	private static final Logger log = LoggerFactory.getLogger(SessionListener.class);

	private String _sourceTopic;

	private MessageProducer _producer;

	private MessageConsumer _consumer;

	public TopicAsQueueListener(Notify sb)
	{
		super();
		_sourceTopic = extractTopicName(sb.destinationName);

		try
		{
			_producer = BrokerProducer.getInstance().getMessageProducer("QUEUE", sb.destinationName);
			_consumer = BrokerConsumer.getInstance().getMessageConsumer(sb.acknowledgeMode, "TOPIC", _sourceTopic);
			_consumer.setMessageListener(this);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public void onMessage(Message amsg)
	{
		try
		{
			_producer.send(amsg);
		}
		catch (Exception e)
		{
			log.error(e.getMessage(), e);
		}
	}

	private String extractTopicName(String destinationName)
	{
		int c_markers = StringUtils.countMatches(destinationName, "@");
		if (c_markers != 1)
		{
			throw new IllegalArgumentException("must have one and only one '@' separator");
		}

		int marker = destinationName.indexOf("@");

		return destinationName.substring(marker + 1);
	}
}
