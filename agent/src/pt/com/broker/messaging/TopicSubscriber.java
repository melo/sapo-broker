package pt.com.broker.messaging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.Gcs;
import pt.com.gcs.messaging.Message;

public class TopicSubscriber extends BrokerListener
{
	private static final Logger log = LoggerFactory.getLogger(TopicSubscriber.class);

	private final List<IoSession> _sessions = new CopyOnWriteArrayList<IoSession>();

	public TopicSubscriber()
	{

	}

	public void onMessage(Message amsg)
	{
		if (amsg == null)
			return;

		try
		{
			for (IoSession ios : _sessions)
			{
				if (ios.getScheduledWriteMessages() < MQ.MAX_PENDING_MESSAGES)
				{
					try
					{
						
						if (ios.isConnected() && !ios.isClosing())
						{
							final SoapEnvelope response = buildNotification(amsg);
							// amsg = null;
							ios.write(response);
						}
						else
						{
							closeConsumer(ios);
						}


					}
					catch (Throwable t)
					{
						closeConsumer(ios);
						try
						{
							(ios.getHandler()).exceptionCaught(ios, t);
						}
						catch (Throwable t1)
						{
							log.error("Could not propagate error to the client session! Message:" + t1.getMessage());
						}
					}
				}
				else
				{
					// Statistics.messageDropped(_appName);
					// FIXME: Write dropped messages to disk
					log.warn("Slow client: \"{}\". message will be discarded. Client Address: {}", amsg.getSourceApp(), ios.getRemoteAddress().toString());
				}
			}

		}
		catch (Throwable e)
		{
			log.error("Error on message Listener: " + e.getMessage(), e);
		}
	}

	private synchronized void closeConsumer(IoSession iosession)
	{
		_sessions.remove(iosession);
		if (_sessions.size()==0)
		{
			Gcs.removeTopicConsumer(this);
			TopicSubscriberList.removeValue(this);
		}
	}

	public void add(IoSession ios)
	{
		_sessions.add(ios);

	}
}
