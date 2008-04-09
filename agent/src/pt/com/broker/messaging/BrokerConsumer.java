package pt.com.broker.messaging;

import org.apache.mina.common.IoSession;
import org.caudexorigo.text.StringUtils;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;

public class BrokerConsumer
{
	private static BrokerConsumer instance = new BrokerConsumer();

	public static BrokerConsumer getInstance()
	{
		return instance;
	}

	private BrokerConsumer()
	{
	}

	public synchronized void listen(Notify sb, IoSession ios)
	{
		try
		{
			QueueSessionListener qsl = QueueSessionListenerList.get(sb.destinationName);
			qsl.addConsumer(ios);
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
			subscriber.addConsumer(ios);
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

	public synchronized void unsubscribe(Unsubscribe unsubs, IoSession session)
	{
		String dname = unsubs.destinationName;
		String dtype = unsubs.destinationType;
		if (dtype.equals("TOPIC"))
		{
			TopicSubscriber subscriber = TopicSubscriberList.get(dname);
			subscriber.removeConsumer(session);
		}
		else if (dtype.equals("QUEUE"))
		{
			QueueSessionListener qsl = QueueSessionListenerList.get(dname);
			qsl.removeConsumer(session);
		}
	}
}