package pt.com.gcs.messaging;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RemoteQueueConsumers
{
	private static final RemoteQueueConsumers instance = new RemoteQueueConsumers();

	private static Logger log = LoggerFactory.getLogger(RemoteQueueConsumers.class);

	protected synchronized static void add(String queueName, IoSession iosession)
	{
		CopyOnWriteArrayList<IoSession> sessions = instance.remoteQueueConsumers.get(queueName);
		if (sessions == null)
		{
			sessions = new CopyOnWriteArrayList<IoSession>();
		}
		if (!sessions.contains(iosession))
		{
			sessions.add(iosession);
		}
		instance.remoteQueueConsumers.put(queueName, sessions);
	}

	protected synchronized static void delete(String queueName)
	{
		instance.remoteQueueConsumers.remove(queueName);
	}

	protected static boolean notify(Message message)
	{
		return instance.doNotify(message);
	}

	protected synchronized static void remove(IoSession iosession)
	{
		Set<String> keys = instance.remoteQueueConsumers.keySet();
		for (String queueName : keys)
		{
			CopyOnWriteArrayList<IoSession> sessions = instance.remoteQueueConsumers.get(queueName);
			if (sessions != null)
			{
				sessions.remove(iosession);
			}
			instance.remoteQueueConsumers.put(queueName, sessions);
		}
	}

	protected synchronized static void remove(String queueName, IoSession iosession)
	{
		CopyOnWriteArrayList<IoSession> sessions = instance.remoteQueueConsumers.get(queueName);
		if (sessions != null)
		{
			sessions.remove(iosession);
		}
		instance.remoteQueueConsumers.put(queueName, sessions);
	}

	protected synchronized static int size(String destinationName)
	{
		CopyOnWriteArrayList<IoSession> sessions = instance.remoteQueueConsumers.get(destinationName);
		if (sessions != null)
		{
			return sessions.size();
		}
		return 0;
	}

	private Map<String, CopyOnWriteArrayList<IoSession>> remoteQueueConsumers = new ConcurrentHashMap<String, CopyOnWriteArrayList<IoSession>>();

	private int currentQEP = 0;

	private Object rr_mutex = new Object();

	private RemoteQueueConsumers()
	{
	}

	protected boolean doNotify(Message message)
	{
		CopyOnWriteArrayList<IoSession> sessions = remoteQueueConsumers.get(message.getDestination());
		if (sessions != null)
		{
			int n = sessions.size();

			if (n > 0)
			{
				IoSession ioSession = pick(sessions);
				if (ioSession != null)
				{
					WriteFuture wf = ioSession.write(message);
					wf.awaitUninterruptibly(120, TimeUnit.SECONDS);
					boolean isWritten = wf.isWritten();
					if (!isWritten)
					{
						ioSession.close();
					}
					return isWritten;
				}
			}
		}

		if (log.isDebugEnabled())
		{
			log.debug("There are no remote consumers for queue: {}", message.getDestination());
		}

		return false;
	}

	private IoSession pick(CopyOnWriteArrayList<IoSession> sessions)
	{
		synchronized (rr_mutex)
		{
			int n = sessions.size();
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
				return sessions.get(currentQEP);
			}
			catch (Throwable t)
			{
				try
				{
					currentQEP = 0;
					return sessions.get(currentQEP);
				}
				catch (Throwable t2)
				{
					return null;
				}

			}
		}
	}
}
