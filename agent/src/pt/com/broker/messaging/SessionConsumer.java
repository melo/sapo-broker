package pt.com.broker.messaging;

import org.apache.mina.core.session.IoSession;

public class SessionConsumer
{
	private IoSession _ioSession;

	private String _consumerName;

	private String _remoteAddress;

	public SessionConsumer(IoSession iosession, String consumerName)
	{
		_ioSession = iosession;
		_consumerName = consumerName;
		_remoteAddress = iosession.getRemoteAddress().toString();
	}

	public String getConsumerName()
	{
		return _consumerName;
	}

	public IoSession getIoSession()
	{
		return _ioSession;
	}

	public String getRemoteAddress()
	{
		return _remoteAddress;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_consumerName == null) ? 0 : _consumerName.hashCode());
		result = prime * result + ((_remoteAddress == null) ? 0 : _remoteAddress.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final SessionConsumer other = (SessionConsumer) obj;
		if (getConsumerName() == null)
		{
			if (other.getConsumerName() != null)
				return false;
		}
		else if (!getConsumerName().equals(other.getConsumerName()))
			return false;
		if (getRemoteAddress() == null)
		{
			if (other.getRemoteAddress() != null)
				return false;
		}
		else if (!getRemoteAddress().equals(other.getRemoteAddress()))
			return false;
		return true;
	}

}
