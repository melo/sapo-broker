package pt.com.broker.client.messaging;

public class Publish
{
	public String actionId;
	
	public BrokerMessage brokerMessage;
	
	public Publish()
	{
		brokerMessage = new BrokerMessage();
	}
}
