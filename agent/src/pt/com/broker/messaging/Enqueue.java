package pt.com.broker.messaging;

public class Enqueue
{
	public String actionId;
	
	public BrokerMessage brokerMessage;
	
	public Enqueue()
	{
		brokerMessage = new BrokerMessage();
	}
}
