package pt.com.broker.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.messaging.DestinationType;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.net.IoSessionHelper;

public class QueueSessionListener extends BrokerListener
{
	private int currentQEP = 0;

	private static final Logger log = LoggerFactory.getLogger(QueueSessionListener.class);

	private final List<IoSession> _sessions = new ArrayList<IoSession>();

	private final String _dname;

	public QueueSessionListener(String destinationName)
	{
		_dname = destinationName;
	}
	
	@Override
	public DestinationType getDestinationType()
	{
		return DestinationType.QUEUE;
	}

	public boolean onMessage(final Message msg)
	{
		if (msg == null)
			return true;
		
		int retryCount = 0;
		final IoSession ioSession = pick();
		
		while (retryCount<5)
		{		
			try
			{
				if (ioSession != null)
				{
					if (ioSession.isConnected() && !ioSession.isClosing())
					{
						final SoapEnvelope response = BrokerListener.buildNotification(msg, "queue");
						WriteFuture future = ioSession.write(response);
						future.awaitUninterruptibly(5000, TimeUnit.MILLISECONDS);

						if (future.isWritten())
						{
							if (log.isDebugEnabled())
							{
								log.debug("Delivered message: {}", msg.getMessageId());
							}

							return true;
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
			retryCount++;
		}


		return false;
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
		log.info("Create message consumer for queue: " + _dname + ", address: " + IoSessionHelper.getRemoteAddress(iosession));
	}

	public void removeConsumer(IoSession iosession)
	{
		synchronized (_sessions)
		{
			if (_sessions.remove(iosession))
				log.info("Remove message consumer for queue: " + _dname + ", address: " + IoSessionHelper.getRemoteAddress(iosession));

			if (_sessions.size() == 0)
			{
				Gcs.removeAsyncConsumer(this);
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
	
	public String getDestinationName()
	{
		return _dname;
	}
}
