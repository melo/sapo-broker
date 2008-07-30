package pt.com.broker.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pt.com.broker.client.messaging.BrokerListener;
import pt.com.broker.client.messaging.BrokerMessage;
import pt.com.broker.client.messaging.Notify;

public class BrokerAsyncConsumer
{
	private final Notify _notify;

	private final BrokerListener _wrappedListener;
	
	private final Pattern _subscriptionName;

	public BrokerAsyncConsumer(Notify notify, BrokerListener listener)
	{
		super();
		_wrappedListener = listener;
		_notify = notify;
		_subscriptionName = Pattern.compile(notify.destinationName);
	}

	public Notify getNotify()
	{
		return _notify;
	}

	public BrokerListener getListener()
	{
		return _wrappedListener;
	}

	
	public boolean deliver(BrokerMessage msg)
	{

		Matcher m = _subscriptionName.matcher(msg.destinationName);
		if (m.matches())
		{
			_wrappedListener.onMessage(msg);
			return true;
		}
		else
		{
			return false;
		}
	}


}
