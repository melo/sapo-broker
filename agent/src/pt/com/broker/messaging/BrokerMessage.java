package pt.com.broker.messaging;

public class BrokerMessage
{
	public DeliveryMode deliveryMode;

	public int priority;

	public String messageId;

	public String correlationId;

	public String timestamp;

	public String expiration;

	public String destinationName;

	public String textPayload;

	public BrokerMessage()
	{
		deliveryMode = DeliveryMode.TRANSIENT;
		priority = 4;
		messageId = "";
		correlationId = "";
		timestamp = "";
		expiration = "";
		destinationName = "";
		textPayload = "";
	}
}
