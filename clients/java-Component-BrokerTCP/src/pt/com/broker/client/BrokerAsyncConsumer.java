package pt.com.broker.client;

import pt.com.broker.client.messaging.BrokerListener;
import pt.com.broker.client.messaging.Notify;

public class BrokerAsyncConsumer
{
	private final Notify _notify;

	private final BrokerListener _listener;

	public BrokerAsyncConsumer(Notify notify, BrokerListener listener)
	{
		super();
		_listener = listener;
		_notify = notify;
	}

	public Notify getNotify()
	{
		return _notify;
	}

	public BrokerListener getListener()
	{
		return _listener;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_listener == null) ? 0 : _listener.hashCode());
		result = prime * result + ((_notify == null) ? 0 : _notify.hashCode());
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
		BrokerAsyncConsumer other = (BrokerAsyncConsumer) obj;
		if (_listener == null)
		{
			if (other._listener != null)
				return false;
		}
		else if (!_listener.equals(other._listener))
			return false;
		if (_notify == null)
		{
			if (other._notify != null)
				return false;
		}
		else if (!_notify.equals(other._notify))
			return false;
		return true;
	}

}
