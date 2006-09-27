package pt.com.broker;

import java.util.HashMap;
import java.util.Map;

public class TopicToQueueDispatcher
{
	private static final TopicToQueueDispatcher instance = new TopicToQueueDispatcher();

	private final Map<String, TopicAsQueueListener> dispatchers;

	private TopicToQueueDispatcher()
	{
		dispatchers = new HashMap<String, TopicAsQueueListener>();
	}

	public static synchronized void add(Notify sb)
	{
		if (instance.dispatchers.get(sb.destinationName) == null)
		{
			instance.dispatchers.put(sb.destinationName , new TopicAsQueueListener(sb));
		}				
	}
	
}
