package pt.com.broker.messaging;

import java.util.List;
import java.util.Random;
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
	private static final Random rnd = new Random();

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

					future.join();
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
		int n = _sessions.size();
		if (n == 0)
			return null;

		int ix = rnd.nextInt(n);
		return _sessions.get(ix);
	}

	public void add(IoSession iosession)
	{
		_sessions.add(iosession);
	}
	
	private synchronized void closeConsumer(IoSession iosession)
	{
		System.out.println("!!!!!! QueueSessionListener._closeConsumer() !!!!!!!!");
		_sessions.remove(iosession);
		if (_sessions.size()==0)
		{
			Gcs.removeQueueConsumer(this);
			QueueSessionListenerList.removeValue(this);
		}
	}
}
