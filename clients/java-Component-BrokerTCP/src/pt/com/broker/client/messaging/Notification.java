package pt.com.broker.client.messaging;

public class Notification
{
	public String actionId;
	
	public BrokerMessage brokerMessage;

	public Notification()
	{
		brokerMessage = new BrokerMessage();
	}

}
