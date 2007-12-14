package pt.com.broker.messaging;

import org.apache.mina.common.IoSession;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.BrokerExecutor;
import pt.com.gcs.messaging.QueueProcessorList;
import pt.com.gcs.tasks.QueueStarter;

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

	public synchronized void listen(Notify sb, IoSession ios)
	{
		try
		{
			QueueSessionListener qsl = QueueSessionListenerList.get(sb.destinationName, sb.acknowledgeMode);
			qsl.addConsumer(ios);

			if (qsl.size() == 1)
			{
				QueueStarter qs = new QueueStarter(QueueProcessorList.get(sb.destinationName));
				BrokerExecutor.execute(qs);
			}
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
		if (unsubs.destinationType.equals("TOPIC"))
		{
			TopicSubscriber subscriber = TopicSubscriberList.get(dname);
			subscriber.removeConsumer(session);
		}
		else if (unsubs.destinationType.equals("QUEUE"))
		{
			QueueSessionListener qsl_a = QueueSessionListenerList.get(dname, AcknowledgeMode.AUTO);
			qsl_a.removeConsumer(session);

			QueueSessionListener qsl_c = QueueSessionListenerList.get(dname, AcknowledgeMode.CLIENT);
			qsl_c.removeConsumer(session);
		}
	}
}