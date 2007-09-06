package pt.com.gcs.messaging;

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Gcs;
import pt.com.gcs.conf.AgentInfo;

public class LocalQueueConsumers
{
	private static final Random rnd = new Random();

	private static Logger log = LoggerFactory.getLogger(LocalQueueConsumers.class);

	private static final LocalQueueConsumers instance = new LocalQueueConsumers();

	public static void add(String queueName, MessageListener listener)
	{
		CopyOnWriteArrayList<MessageListener> listeners = instance.localQueueConsumers.get(queueName);
		if (listeners == null)
		{
			listeners = new CopyOnWriteArrayList<MessageListener>();
		}
		listeners.add(listener);
		instance.localQueueConsumers.put(queueName, listeners);
		instance.broadCastNewQueueConsumer(queueName);
	}

	public static void remove(MessageListener listener)
	{
		Set<String> keys = instance.localQueueConsumers.keySet();
		for (String queueName : keys)
		{
			CopyOnWriteArrayList<MessageListener> listeners = instance.localQueueConsumers.get(queueName);
			if (listeners != null)
			{
				listeners.remove(listener);
			}
			instance.localQueueConsumers.remove(listeners);
			instance.broadCastRemovedQueueConsumer(queueName);
		}
	}

	public static void remove(String queueName, MessageListener listener)
	{
		CopyOnWriteArrayList<MessageListener> listeners = instance.localQueueConsumers.get(queueName);
		if (listeners != null)
		{
			listeners.remove(listener);
		}
		instance.localQueueConsumers.put(queueName, listeners);
	}

	private Map<String, CopyOnWriteArrayList<MessageListener>> localQueueConsumers = new ConcurrentHashMap<String, CopyOnWriteArrayList<MessageListener>>();

	private LocalQueueConsumers()
	{
	}

	public boolean doNotify(Message message)
	{
		CopyOnWriteArrayList<MessageListener> listeners = localQueueConsumers.get(message.getDestination());
		if (listeners != null)
		{
			int n = listeners.size();
			if (n > 0)
			{
				int ix = rnd.nextInt(n);
				MessageListener listener = listeners.get(ix);
				if (listener != null)
				{
					listener.onMessage(message);
					return true;
				}
			}
			return false;
		}
		else
		{
			log.debug("There are no local listeners for queue: {}", message.getDestination());
			return false;
		}
	}

	private void broadCastNewQueueConsumer(String topicName)
	{
		broadCastActionQueueConsumer(topicName, "CREATE");
	}

	private void broadCastRemovedQueueConsumer(String topicName)
	{
		broadCastActionQueueConsumer(topicName, "DELETE");
	}

	public static Set<String> getQueueNameSet()
	{
		return Collections.unmodifiableSet(instance.localQueueConsumers.keySet());
	}

	public static boolean notify(Message message)
	{
		return instance.doNotify(message);
	}

	public static int size()
	{
		return instance.localQueueConsumers.size();
	}

	public static int size(String destinationName)
	{
		CopyOnWriteArrayList<MessageListener> listeners = instance.localQueueConsumers.get(destinationName);
		if (listeners != null)
		{
			listeners.size();
		}
		return 0;
	}

	private void broadCastActionQueueConsumer(String destinationName, String action)
	{
		Set<IoSession> sessions = Gcs.getManagedAcceptorSessions();

		for (IoSession ioSession : sessions)
		{
			broadCastQueueInfo(destinationName, action, ioSession);
		}
	}

	public static void broadCastQueueInfo(String destinationName, String action, IoSession ioSession)
	{
		if (action.equals("CREATE"))
		{
			log.info("Tell {} about new queue consumer for: {}", ioSession.getRemoteAddress().toString(), destinationName);
		}
		else if (action.equals("DELETE"))
		{
			log.info("Tell {} about deleted queue consumer of: {}", ioSession.getRemoteAddress().toString(), destinationName);
		}

		Message m = new Message();
		m.setType(MessageType.SYSTEM_QUEUE);
		String ptemplate = "<sysmessage><action>%s</action><source-name>%s</source-name><source-ip>%s</source-ip><destination>%s</destination></sysmessage>";
		String payload = String.format(ptemplate, action, AgentInfo.getAgentName(), ioSession.getLocalAddress().toString(), destinationName);
		m.setDestination(destinationName);
		m.setContent(payload);
		WriteFuture wf = ioSession.write(m);
		wf.awaitUninterruptibly();
	}

	public static void broadCastAcknowledgement(Message msg, IoSession ioSession)
	{
		log.debug("Acknowledge message with Id: {}", msg.getMessageId());

		//System.out.println(ioSession.getRemoteAddress());
		Message m = new Message();
		m.setType((MessageType.ACK));
		m.setDestination(msg.getDestination());
		m.setAckId(msg.getMessageId());
		WriteFuture wf = ioSession.write(m);
		wf.awaitUninterruptibly();
	}
}
