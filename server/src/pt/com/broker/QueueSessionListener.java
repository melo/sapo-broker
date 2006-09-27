package pt.com.broker;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueSessionListener extends BrokerListener
{
	private static final Logger log = LoggerFactory.getLogger(QueueSessionListener.class);

	private final MessageConsumer _consumer;

	private final IoSession _iosession;

	private final String _appName;

	private final Notify _notify;

	public QueueSessionListener(Notify notify, SessionConsumer sc, MessageConsumer consumer)
	{
		_notify = notify;
		_iosession = sc.getIoSession();
		_appName = sc.getConsumerName();
		_consumer = consumer;
		try
		{
			_consumer.setMessageListener(this);
		}
		catch (Throwable t)
		{
			log.error("Could not create Queue Listener " + t.getMessage(), t);
			throw new RuntimeException(t);
		}
	}

	public void onMessage(final Message amsg)
	{
		try
		{
			Statistics.messageReceived(_appName);

			final TextMessage msg = buildMessage(amsg);

			if (msg == null)
				return;

			if (_notify.acknowledgeMode == AcknowledgeMode.CLIENT)
			{
				WaitingAckMessages.put(msg.getJMSMessageID(), msg);
			}

			final byte[] response = buildNotification(msg);

			if (_iosession.isConnected() && !_iosession.isClosing())
			{
				WriteFuture future = _iosession.write(response);

				future.join();
				if (future.isWritten())
				{
					return;
				}
				else
				{
					_closeConsumer();
				}
			}
		}
		catch (Throwable e)
		{
			try
			{
				(_iosession.getHandler()).exceptionCaught(_iosession, e);
			}
			catch (Throwable t)
			{
				log.error(t.getMessage(), t);
			}
		}
	}

	private void _closeConsumer()
	{
		try
		{
			_consumer.close();
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}
}
