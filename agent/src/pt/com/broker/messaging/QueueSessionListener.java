package pt.com.broker.messaging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.Gcs;
import pt.com.gcs.messaging.Message;

public class QueueSessionListener extends BrokerListener
{
	private int currentQEP = 0;

	private Object rr_mutex = new Object();

	private static final Logger log = LoggerFactory.getLogger(QueueSessionListener.class);

	private final List<IoSession> _sessions = new CopyOnWriteArrayList<IoSession>();

	public QueueSessionListener()
	{
	}

	public void onMessage(final Message msg)
	{
		if (msg == null)
			return;

		IoSession ioSession = pick();
		try
		{
			if (ioSession != null)
			{
				if (ioSession.isConnected() && !ioSession.isClosing())
				{
					final SoapEnvelope response = buildNotification(msg);

					WriteFuture future = ioSession.write(response);

					future.awaitUninterruptibly();
					if (future.isWritten())
					{
						return;
					}
					else
					{
						ioSession.close();
						closeConsumer(ioSession);
					}
				}
				else
				{
					closeConsumer(ioSession);
				}
			}
		}
		catch (Throwable e)
		{
			try
			{
				(ioSession.getHandler()).exceptionCaught(ioSession, e);
				closeConsumer(ioSession);
			}
			catch (Throwable t)
			{
				log.error(t.getMessage(), t);
			}
		}
	}

	private IoSession pick()
	{
		synchronized (rr_mutex)
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
				return _sessions.get(0);
			}
		}
	}

	public void add(IoSession iosession)
	{
		_sessions.add(iosession);
	}

	private synchronized void closeConsumer(IoSession iosession)
	{
		_sessions.remove(iosession);
		if (_sessions.size() == 0)
		{
			Gcs.removeQueueConsumer(this);
			QueueSessionListenerList.removeValue(this);
		}
	}
}
