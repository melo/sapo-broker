package pt.com.gcs.messaging;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.IoSession;
import org.caudexorigo.concurrent.Sleep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RemoteTopicConsumers
{
	private static Logger log = LoggerFactory.getLogger(RemoteTopicConsumers.class);

	private static final RemoteTopicConsumers instance = new RemoteTopicConsumers();

	private Map<String, CopyOnWriteArrayList<IoSession>> remoteTopicConsumers = new ConcurrentHashMap<String, CopyOnWriteArrayList<IoSession>>();
	
	private static final int WRITE_BUFFER_SIZE = 2048*1024;

	private RemoteTopicConsumers()
	{
	}

	public synchronized static void add(String topicName, IoSession iosession)
	{
		log.info("Adding new remote topic consumer for topic:  '{}'", topicName);
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
		if (instance.remoteTopicConsumers.size() > 0)
		{
			String topicName = message.getDestination();
			Set<String> subscriptionNames = instance.remoteTopicConsumers.keySet();

			Set<String> matches = new HashSet<String>();
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

			for (String subscriptionName : matches)
			{
				//message.setDestination(destination);
				instance.doNotify(subscriptionName, message);
			}
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

	private void doNotify(String subscriptionName, Message message)
	{
		try
		{
			CopyOnWriteArrayList<IoSession> sessions = remoteTopicConsumers.get(subscriptionName);
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
						if (ioSession.getScheduledWriteBytes() > WRITE_BUFFER_SIZE)
						{
							Sleep.time(2);
						}
						ioSession.write(message);
					}
				}
			}
			else
			{
				log.info("There are no remote consumers for topic: '{}'", message.getDestination());
			}
		}
		catch (Throwable t)
		{
			log.error(t.getMessage());
		}
	}
}
