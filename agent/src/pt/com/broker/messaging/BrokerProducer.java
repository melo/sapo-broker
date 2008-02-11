package pt.com.broker.messaging;

import static java.lang.System.out;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.caudexorigo.text.DateUtil;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.BrokerExecutor;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageType;
import pt.com.gcs.tasks.GcsExecutor;

public class BrokerProducer
{
	private static final Logger log = LoggerFactory.getLogger(BrokerProducer.class);

	private static BrokerProducer instance = new BrokerProducer();

	public static BrokerProducer getInstance()
	{
		return instance;

	}
		
//	private static final AtomicLong counter = new AtomicLong(0L);
//	
//	final long start = System.currentTimeMillis();
//
//	final Runnable speedcounter = new Runnable()
//	{
//		long t0 = 0L;
//
//		long t1 = 0L;
//
//		public void run()
//		{
//			long time = (System.currentTimeMillis() - start) / 1000;
//			t1 = counter.get();
//			out.print("Throughtput: " + ((t1 - t0) / 10L) + " msg/sec. ");
//			out.print("Total: " + t1 + " messages. ");
//			if (time > 0L)
//				out.println("Avg: " + (t1 / time) + " msg/sec. ");
//			else
//				out.println("Avg: 0 msg/sec. ");
//
//			t0 = t1;
//		}
//	};
	
	private BrokerProducer()
	{
		//BrokerExecutor.scheduleAtFixedRate(speedcounter, 0L, 10L, TimeUnit.SECONDS);
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
				message.setTimestamp(DateUtil.parseISODate(brkMessage.timestamp).getTime());
			}

			if (StringUtils.isNotBlank(brkMessage.expiration))
			{
				message.setExpiration(DateUtil.parseISODate(brkMessage.expiration).getTime());
			}

			message.setContent(brkMessage.textPayload);
			message.setSourceApp(messageSource);

			if (log.isDebugEnabled())
			{
				log.debug("Received message: {}", message.getMessageId());
			}

			return message;
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public void enqueueMessage(Enqueue enqreq, String messageSource)
	{
		BrokerMessage brkm = enqreq.brokerMessage;
		Message msg = prepareForSending(brkm, messageSource);
		msg.setType(MessageType.COM_QUEUE);

		 Gcs.enqueue(msg);
		 //counter.getAndIncrement();
		// Statistics.messageProduced(messageSource);
	}

	public void publishMessage(final Publish pubreq, final String messageSource)
	{
		final BrokerMessage brkm = pubreq.brokerMessage;

//		Runnable publisher = new Runnable()
//		{
//			public void run()
//			{
//				Message msg = prepareForSending(brkm, messageSource);
//				msg.setType(MessageType.COM_TOPIC);
//				Gcs.publish(msg);
//				counter.getAndIncrement();
//				// Statistics.messageProduced(messageSource);
//			}
//		};
//		BrokerExecutor.execute(publisher);
		
		
		Message msg = prepareForSending(brkm, messageSource);
		msg.setType(MessageType.COM_TOPIC);
		Gcs.publish(msg);
		//counter.getAndIncrement();
		// Statistics.messageProduced(messageSource);

	}

	public void acknowledge(Acknowledge ackReq)
	{
		Gcs.ackMessage(ackReq.destinationName, ackReq.messageId);
	}

}
