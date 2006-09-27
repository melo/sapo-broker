package pt.com.broker;

import org.apache.mina.common.IoSession;

public class SessionConsumer
{
	private IoSession _ioSession;
	private String _consumerName;
	
	public SessionConsumer(IoSession iosession, String consumerName)
	{
		_ioSession = iosession;
		_consumerName = consumerName;
	}
	
	public String getConsumerName()
	{
		return _consumerName;
	}
	public IoSession getIoSession()
	{
		return _ioSession;
	}
}
