package pt.com.broker.messaging;

public interface BrokerListener
{	
	public void onMessage(BrokerMessage message);
	
	public boolean isAutoAck();
}
