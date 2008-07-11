package pt.com.broker.client.messaging;

public interface BrokerListener
{	
	public void onMessage(BrokerMessage message);
	
	public boolean isAutoAck();
}
