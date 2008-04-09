package pt.com.broker.messaging;

import org.apache.mina.common.IoSession;

public class QueuePoller implements Runnable
{
	private final Poll _poll;
	private final IoSession _iosession;

	public QueuePoller(Poll poll, IoSession iosession)
	{
		_poll = poll;
		_iosession = iosession;
	}

	@Override
	public void run()
	{
		BrokerSyncConsumer.poll(_poll, _iosession);
	}

}
