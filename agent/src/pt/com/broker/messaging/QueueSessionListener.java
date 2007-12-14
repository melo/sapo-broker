package pt.com.broker.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.Gcs;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.QueueProcessorList;

public class QueueSessionListener extends BrokerListener
{
	private int currentQEP = 0;

	private static final Logger log = LoggerFactory.getLogger(QueueSessionListener.class);

	private final List<IoSession> _sessions = new ArrayList<IoSession>();

	private final AcknowledgeMode _ackMode;

	private final String _dname;

	public QueueSessionListener(String destinationName, AcknowledgeMode ackMode)
	{
		_ackMode = ackMode;
		_dname = destinationName;
	}

	public void onMessage(final Message msg)
	{
		if (msg == null)
			return;

		final IoSession ioSession = pick();
		try
		{
			if (ioSession != null)
			{
				if (ioSession.isConnected() && !ioSession.isClosing())
				{
					final SoapEnvelope response = buildNotification(msg);

					WriteFuture future = ioSession.write(response);

					future.awaitUninterruptibly(100, TimeUnit.MILLISECONDS);
					if (future.isWritten())
					{
						if (log.isDebugEnabled())
						{
							log.debug("Delivered message: {}", msg.getMessageId());
						}

						if (_ackMode == AcknowledgeMode.AUTO)
							Gcs.ackMessage(msg.getMessageId());

						return;
					}
					else
					{
						QueueProcessorList.get(msg.getDestination()).wakeup();
					}
				}
				else
				{
					removeConsumer(ioSession);
				}
			}
		}
		catch (Throwable e)
		{
			try
			{
				(ioSession.getHandler()).exceptionCaught(ioSession, e);
				removeConsumer(ioSession);
			}
			catch (Throwable t)
			{
				log.error(t.getMessage(), t);
			}
		}
	}

	private IoSession pick()
	{
		synchronized (_sessions)
		{
			int n = _sessions.size();
			if (n == 0)
				return null;

			if (currentQEP == (n - 1))
			{
				currentQEP = 0;
			}
			else
			{
				++currentQEP;
			}

			try
			{
				return _sessions.get(currentQEP);
			}
			catch (Exception e)
			{
				currentQEP = 0;
				return _sessions.get(currentQEP);
			}
		}

	}

	public void addConsumer(IoSession iosession)
	{
		synchronized (_sessions)
		{
			_sessions.add(iosession);
		}
		log.info("Create message consumer for queue: " + _dname + ", address: " + iosession.getRemoteAddress());
	}

	public void removeConsumer(IoSession iosession)
	{
		synchronized (_sessions)
		{
			if (_sessions.remove(iosession))
				log.info("Remove message consumer for queue: " + _dname + ", address: " + iosession.getRemoteAddress());

			if (_sessions.size() == 0)
			{
				Gcs.removeQueueConsumer(this);
				QueueSessionListenerList.removeValue(this);
			}
		}
	}

	public synchronized int size()
	{
		synchronized (_sessions)
		{
			return _sessions.size();
		}
	}
}
