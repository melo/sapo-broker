package pt.com.broker.client;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.client.messaging.Acknowledge;
import pt.com.broker.client.messaging.BrokerListener;
import pt.com.broker.client.messaging.BrokerMessage;
import pt.com.broker.client.messaging.CheckStatus;
import pt.com.broker.client.messaging.DestinationType;
import pt.com.broker.client.messaging.Enqueue;
import pt.com.broker.client.messaging.Notify;
import pt.com.broker.client.messaging.Poll;
import pt.com.broker.client.messaging.Publish;
import pt.com.broker.client.messaging.Status;
import pt.com.broker.client.messaging.Unsubscribe;
import pt.com.broker.client.xml.EndPointReference;
import pt.com.broker.client.xml.SoapEnvelope;
import pt.com.broker.client.xml.SoapSerializer;

public class BrokerClient
{
	private static final Logger log = LoggerFactory.getLogger(BrokerClient.class);
	private final String _appName;
	private final ConcurrentMap<String, BrokerListener> _async_listeners = new ConcurrentHashMap<String, BrokerListener>();
	private final BlockingQueue<Status> _bstatus = new LinkedBlockingQueue<Status>();
	private final List<BrokerAsyncConsumer> _consumerList = new CopyOnWriteArrayList<BrokerAsyncConsumer>();
	private BrokerProtocolHandler _netHandler;
	private final String _host;
	private final int _portNumber;

	private final Object mutex = new Object();

	public BrokerClient(String host, int portNumber) throws Throwable
	{
		this(host, portNumber, "brokerClient");
	}

	public BrokerClient(String host, int portNumber, String appName) throws Throwable
	{
		_host = host;
		_portNumber = portNumber;
		_appName = appName;
		_netHandler = new BrokerProtocolHandler(this);
		_netHandler.start();
	}

	public void acknowledge(BrokerMessage brkmsg) throws Throwable
	{
		if ((brkmsg != null) && (StringUtils.isNotBlank(brkmsg.messageId)))
		{
			Acknowledge ack = new Acknowledge();
			ack.messageId = brkmsg.messageId;
			ack.destinationName = brkmsg.destinationName;

			SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/acknowledge");
			soap.body.acknowledge = ack;
			_netHandler.sendMessage(soap);
		}
		else
		{
			throw new IllegalArgumentException("Can't acknowledge invalid message.");
		}
	}

	public void addAsyncConsumer(Notify notify, BrokerListener listener) throws Throwable
	{
		if ((notify != null) && (StringUtils.isNotBlank(notify.destinationName)))
		{
			synchronized (mutex)
			{
				if (_async_listeners.containsKey(notify.destinationName))
				{
					throw new IllegalStateException("A listener for that Destination already exists");
				}

				_async_listeners.put(notify.destinationName, listener);
			}

			String action = buildAction(notify);
			SoapEnvelope soap = buildSoapEnvelope(action);
			soap.body.notify = notify;
			_netHandler.sendMessage(soap);
			_consumerList.add(new BrokerAsyncConsumer(notify, listener));
			log.info("Created new async consumer for '{}'", notify.destinationName);
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Notification request");
		}
	}

	protected void sendSubscriptions() throws Throwable
	{
		for (BrokerAsyncConsumer aconsumer : _consumerList)
		{
			Notify notify = aconsumer.getNotify();
			String action = buildAction(notify);
			SoapEnvelope soap = buildSoapEnvelope(action);
			soap.body.notify = notify;
			_netHandler.sendMessage(soap);
			log.info("Reconnected async consumer for '{}'", notify.destinationName);
		}
	}

	protected void bindConsumers() throws Throwable
	{
		for (BrokerAsyncConsumer bac : _consumerList)
		{
			String action = buildAction(bac.getNotify());
			SoapEnvelope soap = buildSoapEnvelope(action);
			soap.body.notify = bac.getNotify();
			_netHandler.sendMessage(soap);
			_consumerList.add(new BrokerAsyncConsumer(bac.getNotify(), bac.getListener()));
		}
	}

	private String buildAction(Notify notify)
	{
		String raction = "";
		if (notify.destinationType == DestinationType.QUEUE)
		{
			raction = "http://services.sapo.pt/broker/listen";
		}
		else if (notify.destinationType == DestinationType.TOPIC)
		{
			raction = "http://services.sapo.pt/broker/subscribe";
		}
		else if (notify.destinationType == DestinationType.TOPIC_AS_QUEUE)
		{
			raction = "http://services.sapo.pt/broker/listen";
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Notification request");
		}
		return raction;
	}

	private SoapEnvelope buildSoapEnvelope(String action)
	{
		SoapEnvelope soap = new SoapEnvelope();
		soap.header.wsaAction = action;
		soap.header.wsaFrom = new EndPointReference();
		soap.header.wsaFrom.address = _appName;
		soap.header.wsaTo = action;
		return soap;
	}

	public Status checkStatus() throws Throwable
	{
		CheckStatus checkstatus = new CheckStatus();
		SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/checkstatus");
		soap.body.checkStatus = checkstatus;
		_netHandler.sendMessage(soap);
		return _bstatus.take();

	}

	public void close()
	{
		_netHandler.stop();
	}

	public void enqueueMessage(BrokerMessage brkmsg) throws Throwable
	{
		if ((brkmsg != null) && (StringUtils.isNotBlank(brkmsg.destinationName)))
		{
			Enqueue enqreq = new Enqueue();
			//enqreq.actionId = UUID.randomUUID().toString();
			enqreq.brokerMessage = brkmsg;
			SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/enqueue");
			soap.body.enqueue = enqreq;
			_netHandler.sendMessage(soap);
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Enqueue request");
		}

	}

	protected void feedStatusConsumer(Status status) throws Throwable
	{
		_bstatus.offer(status);
	}

	public String getHost()
	{
		return _host;
	}

	public int getPort()
	{
		return _portNumber;
	}

	protected void notifyListener(BrokerMessage msg)
	{
		for (BrokerAsyncConsumer aconsumer : _consumerList)
		{
			boolean isDelivered = aconsumer.deliver(msg);
			BrokerListener listener = aconsumer.getListener();

		
			if (listener.isAutoAck() && isDelivered)
			{
				try
				{
					acknowledge(msg);
				}
				catch (Throwable t)
				{
					log.error("Could not acknowledge message, messageId: '{}'", msg.messageId);
					log.error(t.getMessage(), t);
				}
			}
		}
	}

	public BrokerMessage poll(String queueName) throws Throwable
	{
		if (StringUtils.isNotBlank(queueName))
		{
			Poll p = new Poll();
			p.destinationName = queueName;
			SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/poll");
			soap.body.poll = p;

			SyncConsumer sc = SyncConsumerList.get(queueName);
			sc.increment();

			_netHandler.sendMessage(soap);

			BrokerMessage m = sc.take();
			return m;
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Poll request");
		}
	}

	public void publishMessage(BrokerMessage brkmsg) throws Throwable
	{
		if ((brkmsg != null) && (StringUtils.isNotBlank(brkmsg.destinationName)))
		{
			Publish pubreq = new Publish();
			pubreq.actionId = UUID.randomUUID().toString();
			pubreq.brokerMessage = brkmsg;
			SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/publish");
			soap.body.publish = pubreq;
			_netHandler.sendMessage(soap);
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Publish request");
		}
	}

	public void unsubscribe(DestinationType destinationType, String destinationName) throws Throwable
	{
		if ((StringUtils.isNotBlank(destinationName)) && (destinationType != null))
		{
			Unsubscribe u = new Unsubscribe();
			u.destinationName = destinationName;
			u.destinationType = destinationType;
			SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/unsubscribe");
			soap.body.unsubscribe = u;
			_netHandler.sendMessage(soap);

			for (BrokerAsyncConsumer bac : _consumerList)
			{
				Notify n = bac.getNotify();

				if ((n.destinationName.equals(destinationName)) && (n.destinationType == destinationType))
					_consumerList.remove(bac);
			}
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Unsubscribe request");
		}
	}

	public static void saveToDropbox(String dropboxPath, BrokerMessage brkmsg, DestinationType dtype) throws Throwable
	{
		if ((brkmsg != null) && (StringUtils.isNotBlank(brkmsg.destinationName)) && (StringUtils.isNotBlank(dropboxPath)))
		{
			SoapEnvelope soap = new SoapEnvelope();

			if (dtype == DestinationType.TOPIC)
			{
				Publish pubreq = new Publish();
				pubreq.brokerMessage = brkmsg;
				soap.body.publish = pubreq;
			}
			else if (dtype == DestinationType.QUEUE)
			{
				Enqueue enqreq = new Enqueue();
				enqreq.brokerMessage = brkmsg;
				soap.body.enqueue = enqreq;
			}

			String baseFileName = dropboxPath + File.separator + UUID.randomUUID().toString();
			String tempfileName = baseFileName + ".temp";
			String fileName = baseFileName + ".good";

			FileOutputStream fos = new FileOutputStream(tempfileName);
			SoapSerializer.ToXml(soap, fos);
			fos.flush();
			fos.close();
			(new File(tempfileName)).renameTo(new File(fileName));
		}
		else
		{
			throw new IllegalArgumentException("Missing arguments for Dropbox persistence");
		}
	}
}
