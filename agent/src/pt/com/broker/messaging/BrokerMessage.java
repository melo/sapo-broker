package pt.com.broker.messaging;

public class BrokerMessage
{
	public int priority;

	public String messageId;

	public String correlationId;

	public String timestamp;

	public String expiration;

	public String destinationName;

	public String textPayload;

	public BrokerMessage()
	{
		priority = 4;
		messageId = "";
		correlationId = "";
		timestamp = "";
		expiration = "";
		destinationName = "";
		textPayload = "";
	}
}
