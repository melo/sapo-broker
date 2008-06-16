package pt.com.broker.messaging;

import org.caudexorigo.text.StringUtils;

import pt.com.broker.xml.SoapEnvelope;

public class MQ
{
	// the default maximum message size is 256KB
	private static final int DEFAULT_MAX_MESSAGE_SIZE = 256 * 1024;

	public static final int MAX_MESSAGE_SIZE;

	private static final int DEFAULT_MAX_PENDING_MESSAGES = 50;

	public static final int MAX_PENDING_MESSAGES;

	private static final int DEFAULT_MAX_CONSUMERS = 250;

	public static final int MAX_CONSUMERS;

	private static final int DEFAULT_MAX_PRODUCERS = 250;

	public static final int MAX_PRODUCERS;

	public static final String MANAGEMENT_TOPIC = "/system/management";

	public static final String STATISTICS_TOPIC = "/system/stats";

	public static final String MESSAGE_SOURCE = "MESSAGE_SOURCE";

	public static final String ASYNC_QUEUE_CONSUMER_LIST_ATTR = "ASYNC_QUEUE_CONSUMER_LIST_ATTR";

	public static final String SYNC_QUEUE_CONSUMER_LIST_ATTR = "SYNC_QUEUE_CONSUMER_LIST_ATTR";

	static
	{
		MAX_PENDING_MESSAGES = DEFAULT_MAX_PENDING_MESSAGES;
		MAX_MESSAGE_SIZE = DEFAULT_MAX_MESSAGE_SIZE;
		MAX_CONSUMERS = DEFAULT_MAX_CONSUMERS;
		MAX_PRODUCERS = DEFAULT_MAX_PRODUCERS;
	}

	public static String requestSource(SoapEnvelope soap)
	{
		if (soap.header != null)
			if (soap.header.wsaFrom != null)
				if (StringUtils.isNotBlank(soap.header.wsaFrom.address))
					return soap.header.wsaFrom.address;

		//return "broker://agent/" + pt.com.gcs.conf.GcsInfo.getAgentName() + "/";
		return null;
	}
}
