package pt.com.broker;

public class Denqueue
{
	public String destinationName;

	public long timeOut;

	public AcknowledgeMode acknowledgeMode;
	
	public Denqueue()
	{
		destinationName = "";
		timeOut = 0L;
		acknowledgeMode = AcknowledgeMode.AUTO;
	}	
}
