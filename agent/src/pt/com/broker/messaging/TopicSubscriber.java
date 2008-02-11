package pt.com.broker.messaging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
				if (ios.getScheduledWriteBytes() < (2048 * 1024))
				{
					try
					{
						if (ios.isConnected() && !ios.isClosing())
						{
							final SoapEnvelope response = buildNotification(amsg);
							ios.write(response);

							// final SoapEnvelope response =
							// buildNotification(amsg);
							// long beginWrite = System.currentTimeMillis();
							// WriteFuture wf = ios.write(response);
							// wf.awaitUninterruptibly(2);
							// long endWrite = System.currentTimeMillis();
							// long duration = endWrite - beginWrite;
							// if (duration > 10)
							// {
							// System.out.println("Time for write:" + duration +
							// " ms");
							// }

							// if (wf.isWritten())
							// {
							// if (log.isDebugEnabled())
							// {
							// log.debug("Delivered message: {}",
							// amsg.getMessageId());
							// }
							// //return true;
							// }
							// else
							// {
							// if (log.isDebugEnabled())
							// {
							// log.debug("Slow client: \"{}\". message will be
							// discarded. Client Address: {}",
							// amsg.getSourceApp(),
							// IoSessionHelper.getRemoteAddress(ios));
							// }
							// //return false;
							// }

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
							log.error("Could not propagate error to the client session! Message:" + t1.getMessage());
						}
					}
				}
				else
				{
					// Statistics.messageDropped(_appName);
					log.debug("Slow client: \"{}\". message will be discarded. Client Address: {}", amsg.getSourceApp(), IoSessionHelper.getRemoteAddress(ios));
				}
			}
			return true;
		}
		catch (Throwable e)
		{
			log.error("Error on message Listener: " + e.getMessage(), e);
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
		log.info("Remove message consumer for topic: " + _dname + ", address: " + IoSessionHelper.getRemoteAddress(iosession));
	}

	public void addConsumer(IoSession iosession)
	{
		synchronized (_sessions)
		{
			_sessions.add(iosession);
		}
		log.info("Create message consumer for topic: " + _dname + ", address: " + IoSessionHelper.getRemoteAddress(iosession));
	}
}
