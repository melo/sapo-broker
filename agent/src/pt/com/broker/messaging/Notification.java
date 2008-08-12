package pt.com.broker.messaging;

public class Notification
{
	public String actionId;
	
	public BrokerMessage brokerMessage;

	public Notification()
	{
		brokerMessage = new BrokerMessage();
	}

}
