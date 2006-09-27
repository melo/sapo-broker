package pt.com.broker;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicSubscriber extends BrokerListener
{
	private static final Logger log = LoggerFactory.getLogger(TopicSubscriber.class);

	private final MessageConsumer _consumer;

	private final String _destinationName;

	public TopicSubscriber(MessageConsumer consumer, String destinationName)
	{
		_destinationName = destinationName;
		_consumer = consumer;
		try
		{
			_consumer.setMessageListener(this);
		}
		catch (Throwable t)
		{
			log.error("Could not create Topic Subscriber " + t.getMessage(), t);
			throw new RuntimeException(t);
		}
	}

	public void onMessage(final Message amsg)
	{
		try
		{
			final TextMessage msg = buildMessage(amsg);

			if (msg == null)
				return;

			final byte[] response = buildNotification(msg);

			SessionConsumer[] arr_sessionSubscribers = null;
			List<SessionConsumer> sessionConsumers = SessionTopicConsumerList.get(_destinationName);

			synchronized (sessionConsumers)
			{
				arr_sessionSubscribers = sessionConsumers.toArray(new SessionConsumer[sessionConsumers.size()]);
			}

			for (final SessionConsumer sessionsub : arr_sessionSubscribers)
			{
				IoSession iosession = sessionsub.getIoSession();
				if (iosession.isConnected() && !iosession.isClosing())
				{
					if (iosession.getScheduledWriteRequests() < MQ.MAX_PENDING_MESSAGES)
					{
						try
						{
							iosession.write(response);
							Statistics.messageReceived("TODO://MESSAGE_SOURCE");
						}
						catch (Throwable t)
						{
							try
							{
								(iosession.getHandler()).exceptionCaught(iosession, t);
							}
							catch (Throwable t1)
							{
								log.error("Could not propagate error to the client session! Message:" + t1.getMessage());
							}
						}
					}
					else
					{
						String appName = sessionsub.getConsumerName();
						Statistics.messageDropped(appName);
						if (log.isDebugEnabled())
						{
							log.debug("Slow client: \"" + appName + "\". message will be discarded. Client Address: " + iosession.getRemoteAddress().toString());
						}
					}
				}
				else
				{
					_closeConsumer(iosession);
				}
			}
		}
		catch (Throwable e)
		{
			log.error("Error on message Listener: " + e.getMessage(), e);
		}
	}

	private void _closeConsumer(IoSession iosession)
	{
		final List<SessionConsumer> tc_list = SessionTopicConsumerList.get(_destinationName);
		synchronized (tc_list)
		{
			int index = -1;
			for (SessionConsumer sconsumer : tc_list)
			{
				if (sconsumer.getIoSession().equals(iosession))
				{
					index++;
					break;
				}
			}

			if (index > -1)
			{
				tc_list.remove(index);
			}

			if (tc_list.size() < 1)
			{
				final TopicSubscriber _this = this;
				final Runnable ccloser = new Runnable()
				{
					public void run()
					{
						if (SessionTopicConsumerList.getListSize(_destinationName) < 1)
						{
							TopicSubscriberCache.removeValue(_this);
							try
							{
								_consumer.close();
							}
							catch (JMSException e)
							{
								log.error(e.getMessage(), e);
							}
							BrokerConsumer.decrementConsumers();
						}
					}
				};

				BrokerExecutor.schedule(ccloser, 60000L, TimeUnit.MILLISECONDS);
			}
		}
	}

	public String getDestinationName()
	{
		return _destinationName;
	}

}
