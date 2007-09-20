package pt.com.broker.messaging;

import org.apache.mina.common.IoSession;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.messaging.QueueProcessorList;

public class BrokerConsumer
{
	private static BrokerConsumer instance = new BrokerConsumer();

	private static final Logger log = LoggerFactory.getLogger(BrokerConsumer.class);

	public static BrokerConsumer getInstance()
	{
		return instance;
	}

	private BrokerConsumer()
	{
	}

	public void acknowledge(Acknowledge ackReq)
	{
		WaitingAckMessages.ack(ackReq.messageId);
	}

	public synchronized void listen(Notify sb, IoSession ios)
	{
		try
		{
			QueueSessionListener qsl = QueueSessionListenerList.get(sb.destinationName);
			qsl.add(ios);
			QueueProcessorList.get(sb.destinationName).wakeup();
			log.info("Create asynchronous message consumer for queue : " + sb.destinationName + ", address: " + ios.getRemoteAddress());
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public synchronized void subscribe(Notify sb, IoSession ios)
	{
		if (StringUtils.contains(sb.destinationName, "@"))
		{
			throw new IllegalArgumentException("'@' character not allowed in TOPIC name");
		}

		try
		{
			TopicSubscriber subscriber = TopicSubscriberList.get(sb.destinationName);
			subscriber.add(ios);
			log.info("Create asynchronous message consumer for topic : " + sb.destinationName + ", address: " + ios.getRemoteAddress());
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
}