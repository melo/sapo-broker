package pt.com.broker.client.messaging;

public class Enqueue
{
	public String actionId;
	
	public BrokerMessage brokerMessage;
	
	public Enqueue()
	{
		brokerMessage = new BrokerMessage();
	}
}
