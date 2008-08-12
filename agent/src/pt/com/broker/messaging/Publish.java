package pt.com.broker.messaging;

public class Publish
{
	public String actionId;
	
	public BrokerMessage brokerMessage;
	
	public Publish()
	{
		brokerMessage = new BrokerMessage();
	}
}
