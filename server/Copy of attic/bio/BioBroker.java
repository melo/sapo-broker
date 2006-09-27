package pt.com.manta.bio;

import java.net.Socket;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.mr.api.jms.MantaConnectionFactory;

import pt.com.broker.AcknowledgeRequest;
import pt.com.broker.BrokerApi;
import pt.com.broker.BrokerMessage;
import pt.com.broker.Denqueue;
import pt.com.broker.DenqueueResponse;
import pt.com.broker.Enqueue;
import pt.com.broker.Notify;
import pt.com.broker.Publish;
import pt.com.text.DateUtil;
import pt.com.text.StringUtils;

public class BioBroker implements BrokerApi
{
	private ConnectionFactory _factory;

	private Connection _connection;

	protected Session _auto_ack_session;

	protected Session _client_ack_session;

	private ConcurrentMap<String, MessageConsumer> _consumers = new ConcurrentHashMap<String, MessageConsumer>();

	private ConcurrentMap<String, MessageProducer> _producers = new ConcurrentHashMap<String, MessageProducer>();

	private Map<String, Message> waitingAckMessages = new ConcurrentHashMap<String, Message>();

	private Socket _client;

	public BioBroker(Socket client)
	{
		this();
		_client = client;
	}

	public BioBroker()
	{
		try
		{
			initJMS();
		}
		catch (RemoteException e)
		{
			throw new RuntimeException(e);
		}
	}

	private void initJMS() throws RemoteException
	{
		try
		{
			_factory = new MantaConnectionFactory();
			_connection = _factory.createConnection();
			_auto_ack_session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			_client_ack_session = _connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			_connection.start();
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}

		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			public void run()
			{
				try
				{
					for (MessageConsumer consumer : _consumers.values())
					{
						consumer.close();
					}

					for (MessageProducer producer : _producers.values())
					{
						producer.close();
					}

					_auto_ack_session.close();
					_client_ack_session.close();
					_connection.close();
				}
				catch (JMSException e)
				{
					e.printStackTrace();
				}
			}
		});
	}

	public void acknowledge(AcknowledgeRequest ackReq) throws RemoteException
	{
		Message msg = waitingAckMessages.remove(ackReq.messageId);
		if (msg != null)
		{
			try
			{
				msg.acknowledge();
			}
			catch (JMSException e)
			{
				throw new RemoteException("Error in Manta", e);
			}
		}
		else
		{
			throw new RemoteException("Non existing Message");
		}
	}

	public void enqueueMessage(EnqueueRequest enqreq) throws RemoteException
	{
		try
		{
			BrokerMessage brkm = enqreq.brokerMessage;
			TextMessage message = _auto_ack_session.createTextMessage();
			MessageProducer producer = (MessageProducer) _producers.get(brkm.destinationName);

			if (producer == null)
			{
				Queue queue = _auto_ack_session.createQueue(brkm.destinationName);
				producer = _auto_ack_session.createProducer(queue);
				_producers.put(brkm.destinationName, producer);
			}

			if (StringUtils.isNotBlank(enqreq.brokerMessage.messageId))
				message.setJMSMessageID(enqreq.brokerMessage.messageId);

			if (StringUtils.isNotBlank(enqreq.brokerMessage.correlationId))
				message.setJMSCorrelationID(enqreq.brokerMessage.correlationId);

			if (StringUtils.isNotBlank(enqreq.brokerMessage.timestamp))
			{
				try
				{
					message.setJMSTimestamp(DateUtil.parseISODate(enqreq.brokerMessage.timestamp).getTime());
				}
				catch (ParseException e)
				{
					throw new RuntimeException(e);
				}
			}

			if (StringUtils.isNotBlank(enqreq.brokerMessage.expiration))
			{
				try
				{
					message.setJMSTimestamp(DateUtil.parseISODate(enqreq.brokerMessage.expiration).getTime());
				}
				catch (ParseException e)
				{
					throw new RuntimeException(e);
				}
			}

			message.setText(brkm.textPayload);
			producer.send(message);
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public DenqueueResult denqueueMessage(DenqueueRequest denqreq) throws RemoteException
	{
		if (StringUtils.isBlank(denqreq.destinationName))
		{
			throw new IllegalArgumentException("The Queue name is mandatory.");
		}

		try
		{
			MessageConsumer consumer = (MessageConsumer) _consumers.get(denqreq.destinationName + "#" + denqreq.acknowledgeMode);

			Session mqSession = getSession(denqreq.acknowledgeMode);

			if (consumer == null)
			{
				Queue queue = mqSession.createQueue(denqreq.destinationName);
				consumer = mqSession.createConsumer(queue);
				_consumers.put(denqreq.destinationName + "#" + denqreq.acknowledgeMode, consumer);
			}

			TextMessage msg = (TextMessage) consumer.receive(denqreq.timeOut);

			if (mqSession.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE)
			{
				waitingAckMessages.put(msg.getJMSMessageID(), msg);
			}

			BrokerMessage bkrm = new BrokerMessage();
			if (msg != null)
			{
				bkrm.correlationId = msg.getJMSCorrelationID();
				bkrm.deliveryMode = msg.getJMSDeliveryMode();
				bkrm.destinationName = msg.getJMSDestination().toString();
				bkrm.expiration = DateUtil.formatISODate(new Date(msg.getJMSExpiration()));
				bkrm.messageId = msg.getJMSMessageID();
				bkrm.priority = msg.getJMSPriority();
				bkrm.textPayload = msg.getText();

			}
			DenqueueResult result = new DenqueueResult();
			result.brokerMessage = bkrm;
			return result;
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
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

	public void publishMessage(PublishRequest pubreq) throws RemoteException
	{
		try
		{
			BrokerMessage brkm = pubreq.brokerMessage;
			TextMessage message = _auto_ack_session.createTextMessage();
			MessageProducer producer = _producers.get(brkm.destinationName);

			if (producer == null)
			{
				Topic topic = _auto_ack_session.createTopic(brkm.destinationName);
				producer = _auto_ack_session.createProducer(topic);
				_producers.put(brkm.destinationName, producer);
			}

			if (pubreq.brokerMessage.deliveryMode == 0)
				message.setJMSDeliveryMode(Message.DEFAULT_DELIVERY_MODE);

			if (StringUtils.isNotBlank(pubreq.brokerMessage.messageId))
				message.setJMSMessageID(pubreq.brokerMessage.messageId);

			if (StringUtils.isNotBlank(pubreq.brokerMessage.correlationId))
				message.setJMSCorrelationID(pubreq.brokerMessage.correlationId);

			if (StringUtils.isNotBlank(pubreq.brokerMessage.timestamp))
			{
				try
				{
					message.setJMSTimestamp(DateUtil.parseISODate(pubreq.brokerMessage.timestamp).getTime());
				}
				catch (ParseException e)
				{
					throw new RuntimeException(e);
				}
			}

			if (StringUtils.isNotBlank(pubreq.brokerMessage.expiration))
			{
				try
				{
					message.setJMSTimestamp(DateUtil.parseISODate(pubreq.brokerMessage.expiration).getTime());
				}
				catch (ParseException e)
				{
					throw new RuntimeException(e);
				}
			}

			message.setText(brkm.textPayload);
			producer.send(message);
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public void subscribe(NotificationRequest subreq) throws RemoteException
	{
		String dict_Key = subreq.destinationName + "#" + subreq.acknowledgeMode;
		try
		{
			MessageConsumer consumer = _consumers.get(dict_Key);
			if (consumer != null)
			{
				return;
			}

			Session mqSession = getSession(subreq.acknowledgeMode);
			Topic topic = mqSession.createTopic(subreq.destinationName);
			consumer = mqSession.createConsumer(topic, null, true);

			BioSessionListener listener = new BioSessionListener(this, mqSession.getAcknowledgeMode(), dict_Key);

			consumer.setMessageListener(listener);

			_consumers.put(dict_Key, consumer);
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public void listen(NotificationRequest subreq) throws RemoteException
	{
		String dict_Key = subreq.destinationName + "#" + subreq.acknowledgeMode;
		try
		{
			MessageConsumer consumer = _consumers.get(dict_Key);
			if (consumer != null)
			{
				return;
			}

			Session mqSession = getSession(subreq.acknowledgeMode);
			Queue queue = mqSession.createQueue(subreq.destinationName);

			consumer = mqSession.createConsumer(queue, null, true);

			BioSessionListener listener = new BioSessionListener(this, mqSession.getAcknowledgeMode(), dict_Key);

			consumer.setMessageListener(listener);

			_consumers.put(dict_Key, consumer);
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public void close()
	{
		try
		{
			for (MessageConsumer consumer : _consumers.values())
			{
				consumer.close();
			}

			for (MessageProducer producer : _producers.values())
			{
				producer.close();
			}
			_auto_ack_session.close();
			_client_ack_session.close();
			_connection.close();
		}
		catch (JMSException e)
		{
			e.printStackTrace();
		}

		try
		{
			for (String consumer_name : _consumers.keySet())
			{
				_consumers.remove(consumer_name);
			}

			for (String producer_name : _producers.keySet())
			{
				_producers.remove(producer_name);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public Socket getSocket()
	{
		return _client;
	}

	public Map<String, Message> getWaitingAckMessages()
	{
		return waitingAckMessages;
	}
	
	public void removeConsumer(String consumer_key)
	{
		_consumers.remove(consumer_key);
	}
	
}
