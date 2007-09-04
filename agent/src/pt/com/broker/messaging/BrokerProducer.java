package pt.com.broker.messaging;

import java.text.ParseException;

import org.caudexorigo.text.DateUtil;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Gcs;
import pt.com.gcs.Statistics;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageType;

public class BrokerProducer
{
	private static final Logger log = LoggerFactory.getLogger(BrokerProducer.class);

	private static BrokerProducer instance = new BrokerProducer();

	public static BrokerProducer getInstance()
	{
		return instance;
	}

	private BrokerProducer()
	{

	}

	public void enqueueMessage(Enqueue enqreq, String messageSource)
	{
		BrokerMessage brkm = enqreq.brokerMessage;
		// brkm.deliveryMode = DeliveryMode.PERSISTENT;
		Message msg = prepareForSending(brkm, messageSource);
		Gcs.enqueue(msg);
	}

	private Message prepareForSending(BrokerMessage brkMessage, String messageSource)
	{
		try
		{
			final Message message = new Message();

			if (StringUtils.isNotBlank(brkMessage.messageId))
				message.setMessageId(brkMessage.messageId);

			if (StringUtils.isNotBlank(brkMessage.correlationId))
				message.setCorrelationId(brkMessage.correlationId);
			
			if (StringUtils.isNotBlank(brkMessage.destinationName))
				message.setDestination(brkMessage.destinationName);

			if (StringUtils.isNotBlank(brkMessage.timestamp))
			{
				try
				{
					message.setTimestamp(DateUtil.parseISODate(brkMessage.timestamp).getTime());
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
					message.setTimestamp(DateUtil.parseISODate(brkMessage.expiration).getTime());
				}
				catch (ParseException e)
				{
					throw new RuntimeException(e);
				}
			}

			message.setContent(brkMessage.textPayload);
			message.setSourceApp(messageSource);
			return message;
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public void publishMessage(final Publish pubreq, final String messageSource)
	{
		final BrokerMessage brkm = pubreq.brokerMessage;

		Message msg = prepareForSending(brkm, messageSource);
		msg.setType(MessageType.COM_TOPIC);
		Gcs.publish(msg);
		Statistics.messageProduced(messageSource);
	}

}
