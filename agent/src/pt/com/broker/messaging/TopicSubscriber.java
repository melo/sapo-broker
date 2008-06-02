package pt.com.broker.messaging;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.messaging.DestinationType;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.net.IoSessionHelper;

public class TopicSubscriber extends BrokerListener
{
	private static final Logger log = LoggerFactory.getLogger(TopicSubscriber.class);

	private final Set<IoSession> _sessions = new CopyOnWriteArraySet<IoSession>();

	private final String _dname;

	private final static int MAX_SESSION_BUFFER_SIZE = 2 * 1024 * 1024;

	public TopicSubscriber(String destinationName)
	{
		_dname = destinationName;
	}

	@Override
	public DestinationType getDestinationType()
	{
		return DestinationType.TOPIC;
	}

	public boolean onMessage(Message amsg)
	{
		if (amsg == null)
			return true;

		try
		{
			for (IoSession ios : _sessions)
			{
				try
				{
					if (ios.isConnected() && !ios.isClosing())
					{
						if (ios.getScheduledWriteBytes() > (MAX_SESSION_BUFFER_SIZE))
						{
							if (log.isDebugEnabled())
							{
								log.debug("Slow client: \"{}\". message will be discarded. Client Address: '{}'", amsg.getSourceApp(), IoSessionHelper.getRemoteAddress(ios));
							}
							return false;
						}

						final SoapEnvelope response = BrokerListener.buildNotification(amsg, "topic");
						ios.write(response);
					}
					else
					{
						removeConsumer(ios);
					}
				}
				catch (Throwable t)
				{
					removeConsumer(ios);
					try
					{
						(ios.getHandler()).exceptionCaught(ios, t);
					}
					catch (Throwable t1)
					{
						log.error("Could not propagate error to the client session! Message: {}", t1.getMessage());
					}
				}
			}

			return true;
		}
		catch (Throwable e)
		{
			log.error("Error on message listener for '{}': {}", e.getMessage(), _dname);
		}
		return false;
	}

	public void removeConsumer(IoSession iosession)
	{
		if (_sessions.remove(iosession))
		{
			log.info("Remove message consumer for topic: '{}', address: '{}'", _dname, IoSessionHelper.getRemoteAddress(iosession));
		}
		if (_sessions.size() == 0)
		{
			Gcs.removeAsyncConsumer(this);
			TopicSubscriberList.removeValue(this);
		}
	}

	public void addConsumer(IoSession iosession)
	{
		if (_sessions.add(iosession))
		{
			log.info("Create message consumer for topic: '{}', address: '{}'", _dname, IoSessionHelper.getRemoteAddress(iosession));
		}
	}

	public String getDestinationName()
	{
		return _dname;
	}
}
