package pt.com.broker;

import java.text.ParseException;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.manta.MQLink;
import pt.com.text.DateUtil;
import pt.com.text.StringUtils;

public class BrokerProducer
{
	private static final Logger log = LoggerFactory.getLogger(BrokerProducer.class);

	private static BrokerProducer instance = new BrokerProducer();

	public static BrokerProducer getInstance()
	{
		return instance;
	}

	private Connection _connection;

	protected Session _session;

	private BrokerProducer()
	{
		try
		{
			initJMS();
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public void enqueueMessage(Enqueue enqreq, String messageSource)
	{
		BrokerMessage brkm = enqreq.brokerMessage;
		MessageProducer producer = getMessageProducer("QUEUE", brkm.destinationName);
		brkm.deliveryMode = DeliveryMode.PERSISTENT;
		final TextMessage txtMsg = prepareForSending(brkm, messageSource);
		send(txtMsg, producer, messageSource);
	}

	protected MessageProducer getMessageProducer(String destinationType, String destinationName)
	{
		if (MessageProducerCache.size() < MQ.MAX_PRODUCERS)
		{
			String pkey = destinationType + "$" + destinationName;

			MessageProducer producer;
			try
			{
				producer = MessageProducerCache.get(pkey, _session);
			}
			catch (Throwable e)
			{
				log.error("Couldn't get a Message Producer from the cache:" + e.getMessage(), e);
				throw new RuntimeException(e);
			}
			return producer;
		}
		else
		{
			throw new RuntimeException("The maximum number of message producers has been reached.");
		}
	}

	private void initJMS() throws JMSException
	{
		_connection = MQLink.getJMSConnection();
		_session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	private TextMessage prepareForSending(BrokerMessage brkMessage, String messageSource)
	{
		try
		{
			final TextMessage message = _session.createTextMessage();

			if (brkMessage.deliveryMode == null)
			{
				message.setJMSDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);
			}
			else
			{
				message.setJMSDeliveryMode(brkMessage.deliveryMode.getValue());
			}

			if (StringUtils.isNotBlank(brkMessage.messageId))
				message.setJMSMessageID(brkMessage.messageId);

			if (StringUtils.isNotBlank(brkMessage.correlationId))
				message.setJMSCorrelationID(brkMessage.correlationId);

			if (StringUtils.isNotBlank(brkMessage.timestamp))
			{
				try
				{
					message.setJMSTimestamp(DateUtil.parseISODate(brkMessage.timestamp).getTime());
				}
				catch (ParseException e)
				{
					throw new RuntimeException(e);
				}
			}

			if (StringUtils.isNotBlank(brkMessage.expiration))
			{
				try
				{
					message.setJMSTimestamp(DateUtil.parseISODate(brkMessage.expiration).getTime());
				}
				catch (ParseException e)
				{
					throw new RuntimeException(e);
				}
			}

			message.setText(brkMessage.textPayload);
			tagMessage(message, messageSource);
			return message;
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public void publishMessage(Publish pubreq, String messageSource)
	{
		BrokerMessage brkm = pubreq.brokerMessage;
		MessageProducer producer = getMessageProducer("TOPIC", brkm.destinationName);
		brkm.deliveryMode = DeliveryMode.PERSISTENT;
		final TextMessage txtMsg = prepareForSending(brkm, messageSource);
		send(txtMsg, producer, messageSource);

		final String destinationName = brkm.destinationName;
		final Runnable dispatcher = new Runnable()
		{
			public void run()
			{
				TopicToQueueDispatcher.forward(destinationName, txtMsg);
			}
		};

		BrokerExecutor.execute(dispatcher);
	}

	private void send(TextMessage txtMessage, MessageProducer producer, String messageSource)
	{
		try
		{
			if (log.isDebugEnabled())
			{
				log.debug("Send message with payload: " + txtMessage.getText());
			}

			producer.send(txtMessage);

			Statistics.messageProduced(messageSource);
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	private void tagMessage(Message msg, String tag)
	{
		try
		{
			msg.setStringProperty(MQ.MESSAGE_SOURCE, tag);
		}
		catch (Throwable e)
		{
			throw new RuntimeException("Error setting message source", e);
		}
	}
}
