package pt.com.broker;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.manta.MQLink;
import pt.com.text.DateUtil;
import pt.com.text.StringUtils;

public class BrokerConsumer
{
	private static BrokerConsumer instance = new BrokerConsumer();

	private static final Logger log = LoggerFactory.getLogger(BrokerConsumer.class);

	public static void decrementConsumers()
	{
		instance._nrConsumers.decrementAndGet();
	}

	public static BrokerConsumer getInstance()
	{
		return instance;
	}

	private Connection _connection;

	protected Session _auto_ack_session;

	protected Session _client_ack_session;

	private AtomicInteger _nrConsumers = new AtomicInteger(1);

	private BrokerConsumer()
	{
		try
		{
			initJMS();
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public void acknowledge(Acknowledge ackReq)
	{
		WaitingAckMessages.ack(ackReq.messageId);
	}

	public void closeSession(IoSession iosession)
	{
		synchronized (iosession)
		{
			try
			{
				ArrayList aQueueConsumers = (ArrayList) iosession.getAttribute(MQ.ASYNC_QUEUE_CONSUMER_LIST_ATTR);
				for (Iterator iter = aQueueConsumers.iterator(); iter.hasNext();)
				{
					MessageConsumer aqmc = (MessageConsumer) iter.next();
					try
					{
						aqmc.close();
					}
					catch (Throwable e)
					{
						// IGNORE
					}
					_nrConsumers.decrementAndGet();
				}

				ArrayList sQueueConsumers = (ArrayList) iosession.getAttribute(MQ.SYNC_QUEUE_CONSUMER_LIST_ATTR);
				for (Iterator iter = sQueueConsumers.iterator(); iter.hasNext();)
				{
					MessageConsumer sqmc = (MessageConsumer) iter.next();
					try
					{
						sqmc.close();
					}
					catch (Throwable e)
					{
						// IGNORE
					}
					_nrConsumers.decrementAndGet();
				}
			}

			catch (Throwable e)
			{
				log.error(e.getMessage(), e);
			}
		}
	}

	public DenqueueResultWrapper denqueueMessage(final Denqueue denqreq, IoSession iosession)
	{
		if (StringUtils.isBlank(denqreq.destinationName))
		{
			throw new IllegalArgumentException("The Queue name is mandatory.");
		}

		try
		{
			String dkey = "QUEUE::" + denqreq.destinationName + "#" + denqreq.acknowledgeMode.toString();

			MessageConsumer consumer;

			if (iosession.getAttribute(dkey) == null)
			{
				consumer = getMessageConsumer(denqreq.acknowledgeMode, "QUEUE", denqreq.destinationName);
				ArrayList sQueueConsumerList = (ArrayList) iosession.getAttribute(MQ.SYNC_QUEUE_CONSUMER_LIST_ATTR);
				sQueueConsumerList.add(consumer);
				iosession.setAttribute(dkey, consumer);
			}
			else
			{
				consumer = (MessageConsumer) iosession.getAttribute(dkey);
			}

			TextMessage msg = (TextMessage) consumer.receive(denqreq.timeOut);
			String sourceAgent = msg.getStringProperty("SourceAgent");

			if (denqreq.acknowledgeMode == AcknowledgeMode.CLIENT)
			{
				WaitingAckMessages.put(msg.getJMSMessageID(), msg);
			}

			BrokerMessage bkrm = new BrokerMessage();
			if (msg != null)
			{
				bkrm.correlationId = msg.getJMSCorrelationID();
				bkrm.deliveryMode = DeliveryMode.lookup(msg.getJMSDeliveryMode());
				bkrm.destinationName = msg.getJMSDestination().toString();
				bkrm.expiration = DateUtil.formatISODate(new Date(msg.getJMSExpiration()));
				bkrm.messageId = msg.getJMSMessageID();
				bkrm.priority = msg.getJMSPriority();
				bkrm.textPayload = msg.getText();

			}

			DenqueueResponse dresult = new DenqueueResponse();
			dresult.brokerMessage = bkrm;
			DenqueueResultWrapper drw = new DenqueueResultWrapper();
			drw.dresult = dresult;
			drw.sourceAgent = sourceAgent;

			return drw;
		}
		catch (Throwable e)
		{
			if (e instanceof InterruptedException)
			{
				Thread.currentThread().interrupt();
			}
			throw new RuntimeException(e);
		}
	}

	public MessageConsumer getMessageConsumer(AcknowledgeMode ackMode, String destinationType, String destinationName)
	{
		if (_nrConsumers.get() < MQ.MAX_CONSUMERS)
		{
			try
			{
				Session mqSession = getSession(ackMode.getValue());

				MessageConsumer consumer = null;
				if (destinationType.equals("TOPIC"))
				{
					Topic topic = BrokerDestination.getTopic(destinationName, mqSession);
					consumer = mqSession.createConsumer(topic);
				}
				else if ((destinationType.equals("QUEUE")) || (destinationType.equals("TOPIC_AS_QUEUE")))
				{
					Queue queue = BrokerDestination.getQueue(destinationName, mqSession);
					consumer = mqSession.createConsumer(queue, null, true);
				}
				else
				{
					throw new IllegalArgumentException("Not a valid destination type!");
				}
				_nrConsumers.incrementAndGet();
				log.info("Create Message Consumer for : " + destinationName);
				return consumer;
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
		else
		{
			throw new RuntimeException("The maximum number of message consumers has been reached.");
		}
	}

	private Session getSession(int ackMode)
	{
		if (ackMode == Session.CLIENT_ACKNOWLEDGE)
		{
			return _client_ack_session;
		}
		return _auto_ack_session;
	}

	private void initJMS() throws Throwable
	{
		_connection = MQLink.getJMSConnection();
		_auto_ack_session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		_client_ack_session = _connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
	}

	public synchronized void listen(Notify sb, SessionConsumer sc)
	{
		try
		{
			IoSession iosession = sc.getIoSession();
			MessageConsumer consumer = getMessageConsumer(sb.acknowledgeMode, sb.destinationType, sb.destinationName);
			QueueSessionListener listener = new QueueSessionListener(sb, sc, consumer);
			ArrayList aQueueConsumerList = (ArrayList) iosession.getAttribute(MQ.ASYNC_QUEUE_CONSUMER_LIST_ATTR);
			aQueueConsumerList.add(consumer);

			log.info("Create asynchronous message consumer for queue : " + sb.destinationName + ", address: " + iosession.getRemoteAddress());
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public synchronized void subscribe(Notify sb, SessionConsumer sc)
	{
		if (StringUtils.contains(sb.destinationName, "@"))
		{
			throw new IllegalArgumentException("'@' character not allowed in TOPIC name");
		}

		try
		{
			List<SessionConsumer> sct_list = SessionTopicConsumerList.get(sb.destinationName);
			synchronized (sct_list)
			{
				sct_list.add(sc);
			}			
			IoSession iosession = sc.getIoSession();
			TopicSubscriber subscriber = TopicSubscriberCache.get(sb.destinationName);

			log.info("Create asynchronous message consumer for topic : " + sb.destinationName + ", address: " + iosession.getRemoteAddress());
		}
		catch (Throwable e)
		{
			if (e instanceof InterruptedException)
			{
				Thread.currentThread().interrupt();
			}
			throw new RuntimeException(e);
		}
	}
}