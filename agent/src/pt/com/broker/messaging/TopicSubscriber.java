package pt.com.broker.messaging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.net.IoSessionHelper;

public class TopicSubscriber extends BrokerListener
{
	private static final Logger log = LoggerFactory.getLogger(TopicSubscriber.class);

	private final List<IoSession> _sessions = new CopyOnWriteArrayList<IoSession>();

	private final String _dname;
	
	private static final int MAX_WAIT_TIME = 500;

	public TopicSubscriber(String destinationName)
	{
		_dname = destinationName;
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
							final SoapEnvelope response = buildNotification(amsg, "topic");
							WriteFuture wf = ios.write(response);
							boolean isWriten = wf.awaitUninterruptibly(MAX_WAIT_TIME, TimeUnit.MILLISECONDS);
							
							if (log.isDebugEnabled())
							{
								if (!isWriten)
								{
									log.debug("Slow client: \"{}\". message will be discarded. Client Address: '{}'", amsg.getSourceApp(), IoSessionHelper.getRemoteAddress(ios));
								}								
							}							
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
			log.error("Error on message Listener: '{}'", e.getMessage(), e);
		}
		return false;
	}

	public void removeConsumer(IoSession iosession)
	{
		synchronized (_sessions)
		{
			_sessions.remove(iosession);
			if (_sessions.size() == 0)
			{
				Gcs.removeTopicConsumer(this);
				TopicSubscriberList.removeValue(this);
			}
		}
		log.info("Remove message consumer for topic: '{}', address: '{}'", _dname, IoSessionHelper.getRemoteAddress(iosession));
	}

	public void addConsumer(IoSession iosession)
	{
		synchronized (_sessions)
		{
			_sessions.add(iosession);
		}
		log.info("Create message consumer for topic: '{}', address: '{}'", _dname, IoSessionHelper.getRemoteAddress(iosession));
	}
	
	public String getDestinationName()
	{
		return _dname;
	}
}
