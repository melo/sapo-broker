package pt.com.gcs.messaging;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteQueueConsumers
{
	private int currentQEP = 0;

	private Object rr_mutex = new Object();

	private static final RemoteQueueConsumers instance = new RemoteQueueConsumers();

	private static Logger log = LoggerFactory.getLogger(RemoteQueueConsumers.class);

	public synchronized static void add(String queueName, IoSession iosession)
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

	public static boolean notify(Message message)
	{
		return instance.doNotify(message);
	}

	public synchronized static void remove(IoSession iosession)
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

	public synchronized static void remove(String queueName, IoSession iosession)
	{
		CopyOnWriteArrayList<IoSession> sessions = instance.remoteQueueConsumers.get(queueName);
		if (sessions != null)
		{
			sessions.remove(iosession);
		}
		instance.remoteQueueConsumers.put(queueName, sessions);
	}

	public static int size()
	{
		return instance.remoteQueueConsumers.size();
	}

	public synchronized static int size(String destinationName)
	{
		CopyOnWriteArrayList<IoSession> sessions = instance.remoteQueueConsumers.get(destinationName);
		if (sessions != null)
		{
			return sessions.size();
		}
		return 0;
	}

	private Map<String, CopyOnWriteArrayList<IoSession>> remoteQueueConsumers = new ConcurrentHashMap<String, CopyOnWriteArrayList<IoSession>>();

	private RemoteQueueConsumers()
	{
	}

	public boolean doNotify(Message message)
	{
		CopyOnWriteArrayList<IoSession> sessions = remoteQueueConsumers.get(message.getDestination());
		if (sessions != null)
		{
			int n = sessions.size();

			if (n > 0)
			{
				int ix = getNextEnpoint(n);
				IoSession ioSession = sessions.get(ix);
				if (ioSession != null)
				{
					// System.out.println("RemoteQueueConsumers.doNotify().ioSession.getScheduledWriteMessages():
					// " + ioSession.getScheduledWriteMessages());
					WriteFuture wf = ioSession.write(message);
					wf.awaitUninterruptibly();
					return wf.isWritten();
				}
				return false;
			}
			return false;
		}
		else
		{
			log.debug("There are no remote consumers for queue: {}", message.getDestination());
			return false;
		}
	}

	private int getNextEnpoint(int size)
	{
		synchronized (rr_mutex)
		{

			if (currentQEP == size - 1)
			{
				return 0;
			}
			else
			{
				return ++currentQEP;
			}
		}
	}
}
