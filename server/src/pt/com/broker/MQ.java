package pt.com.broker;

import org.mr.MantaAgent;
import org.mr.core.configuration.ConfigManager;

import pt.com.text.StringUtils;
import pt.com.xml.SoapEnvelope;

public class MQ
{
	// the default maximum message size is 256KB
	private static final int DEFAULT_MAX_MESSAGE_SIZE = 256 * 1024;

	public static final int MAX_MESSAGE_SIZE;

	private static final int DEFAULT_MAX_PENDING_MESSAGES = 25;

	public static final int MAX_PENDING_MESSAGES;

	private static final int DEFAULT_MAX_CONSUMERS = 250;

	public static final int MAX_CONSUMERS;

	private static final int DEFAULT_MAX_PRODUCERS = 250;

	public static final int MAX_PRODUCERS;

	public static final String MANAGEMENT_TOPIC = "/system/management";

	public static final String STATISTICS_TOPIC = "/system/stats";

	public static final String DISPATCHER_TOPIC = "/system/dispatcher";

	public static final String MESSAGE_SOURCE = "MESSAGE_SOURCE";

	public static final String ASYNC_QUEUE_CONSUMER_LIST_ATTR = "ASYNC_QUEUE_CONSUMER_LIST_ATTR";

	public static final String SYNC_QUEUE_CONSUMER_LIST_ATTR = "SYNC_QUEUE_CONSUMER_LIST_ATTR";

	static
	{
		ConfigManager config = MantaAgent.getInstance().getSingletonRepository().getConfigManager();
		MAX_PENDING_MESSAGES = config.getIntProperty("sapo:agent.maximum_pending_messages", DEFAULT_MAX_PENDING_MESSAGES);
		MAX_MESSAGE_SIZE = config.getIntProperty("sapo:agent.maximum_message_size", DEFAULT_MAX_MESSAGE_SIZE);
		MAX_CONSUMERS = config.getIntProperty("sapo:agent.maximum_message_consumers", DEFAULT_MAX_CONSUMERS);
		MAX_PRODUCERS = config.getIntProperty("sapo:agent.maximum_message_producers", DEFAULT_MAX_PRODUCERS);
	}

	public static String requestSource(SoapEnvelope soap)
	{
		if (soap.header != null)
			if (soap.header.wsaFrom != null)
				if (StringUtils.isNotBlank(soap.header.wsaFrom.address))
					return soap.header.wsaFrom.address;

		return "broker://agent/" + AgentInfo.AGENT_NAME + "/";
	}
}
