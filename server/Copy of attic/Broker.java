package pt.com.broker;

import java.rmi.RemoteException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.mina.common.IoSession;
import org.mr.MantaAgent;
import org.mr.core.util.UUID;
import org.mr.kernel.services.MantaService;
import org.mr.kernel.world.WorldModeler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.manta.MQLink;
import pt.com.manta.MantaBusServer;
import pt.com.text.DateUtil;
import pt.com.text.StringUtils;

public class Broker implements BrokerApi
{
	private static final Logger log = LoggerFactory.getLogger(Broker.class);

	private Connection _connection;

	protected Session _auto_ack_session;

	protected Session _client_ack_session;

	private ConcurrentMap<String, MessageConsumer> _consumers = new ConcurrentHashMap<String, MessageConsumer>();

	private ConcurrentMap<String, MessageProducer> _producers = new ConcurrentHashMap<String, MessageProducer>();

	private Map<String, Message> waitingAckMessages = new ConcurrentHashMap<String, Message>();

	private IoSession _iosession;

	private MQLink _mqlink = MQLink.getInstance();

	public Broker(IoSession iosession)
	{
		this();
		_iosession = iosession;
	}

	public Broker()
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
			_connection = _mqlink.getJMSConnection();
			_auto_ack_session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			_client_ack_session = _connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		}
		catch (Exception e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public void acknowledge(Acknowledge ackReq) throws RemoteException
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

	public void enqueueMessage(Enqueue enqreq) throws RemoteException
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
			tagMessage(message);
			producer.send(message);
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public DenqueueResultWrapper denqueueMessage(Denqueue denqreq) throws RemoteException
	{
		if (StringUtils.isBlank(denqreq.destinationName))
		{
			throw new IllegalArgumentException("The Queue name is mandatory.");
		}

		try
		{
			MessageConsumer consumer = (MessageConsumer) _consumers.get(denqreq.destinationName + "#" + denqreq.acknowledgeMode);

			Session mqSession = getSession(denqreq.acknowledgeMode.getValue());

			if (consumer == null)
			{
				Queue queue = mqSession.createQueue(denqreq.destinationName);
				consumer = mqSession.createConsumer(queue);
				_consumers.put(denqreq.destinationName + "#" + denqreq.acknowledgeMode, consumer);
			}

			TextMessage msg = (TextMessage) consumer.receive(denqreq.timeOut);
			String sourceAgent = msg.getStringProperty("SourceAgent");

			if (mqSession.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE)
			{
				waitingAckMessages.put(msg.getJMSMessageID(), msg);
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
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	private Session getSession(int ackMode)
	{
		try
		{
			if (ackMode == Session.CLIENT_ACKNOWLEDGE)
			{

				_auto_ack_session.close();

				return _client_ack_session;
			}
			_client_ack_session.close();
			return _auto_ack_session;

		}
		catch (JMSException e)
		{
			throw new RuntimeException("Error in Manta", e);
		}
	}

	public void publishMessage(Publish pubreq) throws RemoteException
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

			if (pubreq.brokerMessage.deliveryMode == null)
			{
				message.setJMSDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);
			}
			else
			{
				message.setJMSDeliveryMode(pubreq.brokerMessage.deliveryMode.getValue());
			}

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
			tagMessage(message);
			producer.send(message);
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public void subscribe(Notify subreq) throws RemoteException
	{
		if (StringUtils.contains(subreq.destinationName, "@"))
		{
			throw new RemoteException("'@' character not allowed in TOPIC name");
		}

		String dict_Key = subreq.destinationName + "$" + subreq.acknowledgeMode;
		try
		{
			MessageConsumer consumer = _consumers.get(dict_Key);
			if (consumer != null)
			{
				return;
			}

			Session mqSession = getSession(subreq.acknowledgeMode.getValue());
			Topic topic = mqSession.createTopic(subreq.destinationName);
			consumer = mqSession.createConsumer(topic, null, true);

			MessageListener listener = new SessionListener(this, mqSession.getAcknowledgeMode());
			consumer.setMessageListener(listener);
			_consumers.put(dict_Key, consumer);						
		}
		catch (JMSException e)
		{
			throw new RemoteException("Error in Manta", e);
		}
	}

	public void listen(Notify subreq) throws RemoteException
	{
		String dict_Key = subreq.destinationName + "$" + subreq.acknowledgeMode;
		try
		{
			MessageConsumer consumer = _consumers.get(dict_Key);
			if (consumer != null)
			{
				return;
			}

			Session mqSession = getSession(subreq.acknowledgeMode.getValue());
			Queue queue = mqSession.createQueue(subreq.destinationName);

			consumer = mqSession.createConsumer(queue, null, true);

			SessionListener listener = new SessionListener(this, mqSession.getAcknowledgeMode());

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.mr.api.rmi.thin.ThinMessagingInterface#getTopics()
	 */
	public Destination[] getTopics() throws RemoteException
	{
		WorldModeler world = MantaAgent.getInstance().getSingletonRepository().getWorldModeler();

		ArrayList list = new ArrayList();
		Iterator iter = world.getServices(world.getDefaultDomainName()).iterator();
		while (iter.hasNext())
		{
			MantaService service = (MantaService) iter.next();
			if (service.getServiceType() == MantaService.SERVICE_TYPE_TOPIC)
			{
				Destination dt = new Destination();
				dt.name = service.getServiceName();
				list.add(dt);
			}
		}
		Destination[] result = new Destination[list.size()];
		for (int i = 0; i < list.size(); i++)
		{
			result[i] = (Destination) list.get(i);
		}
		return result;
	}

	public Destination[] getQueues() throws RemoteException
	{
		WorldModeler world = MantaAgent.getInstance().getSingletonRepository().getWorldModeler();

		ArrayList list = new ArrayList();
		Iterator iter = world.getServices(world.getDefaultDomainName()).iterator();
		while (iter.hasNext())
		{
			MantaService service = (MantaService) iter.next();
			if (service.getServiceType() == MantaService.SERVICE_TYPE_QUEUE)
			{
				Destination dt = new Destination();
				dt.name = service.getServiceName();
				list.add(dt);
			}
		}
		Destination[] result = new Destination[list.size()];
		for (int i = 0; i < list.size(); i++)
		{
			result[i] = (Destination) list.get(i);
		}
		return result;
	}

	public IoSession getIosession()
	{
		return _iosession;
	}

	public Map<String, Message> getWaitingAckMessages()
	{
		return waitingAckMessages;
	}

	private void tagMessage(Message msg)
	{
		try
		{
			msg.setStringProperty("SourceAgent", MantaBusServer.AGENT_NAME);
			msg.setStringProperty("uniqueId", UUID.randomUUID().toString());
		}
		catch (JMSException e)
		{
			throw new RuntimeException("Error setting message source", e);
		}
	}

}
