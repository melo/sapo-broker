package pt.com.gcs.messaging;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RemoteTopicConsumers
{
	private static Logger log = LoggerFactory.getLogger(RemoteTopicConsumers.class);

	private static final RemoteTopicConsumers instance = new RemoteTopicConsumers();

	private Map<String, CopyOnWriteArrayList<IoSession>> remoteTopicConsumers = new ConcurrentHashMap<String, CopyOnWriteArrayList<IoSession>>();

	private RemoteTopicConsumers()
	{
	}

	public synchronized static void add(String topicName, IoSession iosession)
	{
		log.info("Adding new remote topic consumer for topic:  {}", topicName);
		try
		{
			CopyOnWriteArrayList<IoSession> sessions = instance.remoteTopicConsumers.get(topicName);
			if (sessions == null)
			{
				sessions = new CopyOnWriteArrayList<IoSession>();
			}
			if (!sessions.contains(iosession))
			{
				sessions.add(iosession);
			}
			instance.remoteTopicConsumers.put(topicName, sessions);
		}
		catch (Throwable t)
		{
			log.error(t.getMessage());
		}
	}

	public static void notify(Message message)
	{
		String topicName = message.getDestination();
		Set<String> subscriptionNames = instance.remoteTopicConsumers.keySet();

		List<String> matches = new CopyOnWriteArrayList<String>();
		for (String sname : subscriptionNames)
		{
			if (sname.equals(topicName))
			{
				matches.add(topicName);
			}
			else
			{
				if (TopicMatcher.match(sname, topicName))
					matches.add(sname);
			}
		}

		for (String destination : matches)
		{
			message.setDestination(destination);
			instance.doNotify(message);
		}

	}

	public synchronized static void remove(IoSession iosession)
	{
		try
		{
			Set<String> keys = instance.remoteTopicConsumers.keySet();
			for (String topicName : keys)
			{
				CopyOnWriteArrayList<IoSession> sessions = instance.remoteTopicConsumers.get(topicName);
				if (sessions != null)
				{
					sessions.remove(iosession);
				}
				instance.remoteTopicConsumers.put(topicName, sessions);
			}
		}
		catch (Throwable t)
		{
			log.error(t.getMessage());
		}
	}

	public synchronized static void remove(String topicName, IoSession iosession)
	{
		try
		{
			CopyOnWriteArrayList<IoSession> sessions = instance.remoteTopicConsumers.get(topicName);
			if (sessions != null)
			{
				sessions.remove(iosession);
			}
			instance.remoteTopicConsumers.put(topicName, sessions);
		}
		catch (Throwable t)
		{
			log.error(t.getMessage());
		}
	}

	public synchronized static int size()
	{
		return instance.remoteTopicConsumers.size();
	}

	public synchronized static int size(String destinationName)
	{
		CopyOnWriteArrayList<IoSession> sessions = instance.remoteTopicConsumers.get(destinationName);
		if (sessions != null)
		{
			return sessions.size();
		}
		return 0;
	}

	private void doNotify(Message message)
	{
		try
		{
			CopyOnWriteArrayList<IoSession> sessions = remoteTopicConsumers.get(message.getDestination());
			if (sessions != null)
			{
				if (sessions.size() == 0)
				{
					log.debug("There are no remote peers to deliver the message.");
					return;
				}

				log.debug("There are {} remote peer(s) to deliver the message.", sessions.size());

				for (IoSession ioSession : sessions)
				{
					if (ioSession != null)
					{
						if (ioSession.getScheduledWriteBytes()<1048576)
						{
							ioSession.write(message);
						}
						
					}

				}
			}
			else
			{
				log.info("There are no remote consumers for topic: {}", message.getDestination());
			}
		}
		catch (Throwable t)
		{
			log.error(t.getMessage());
		}
	}
}
