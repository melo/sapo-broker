package pt.com.gcs.messaging;

public interface MessageListener
{
	public boolean onMessage(Message message);

	public String getDestinationName();

	public DestinationType getDestinationType();

}
