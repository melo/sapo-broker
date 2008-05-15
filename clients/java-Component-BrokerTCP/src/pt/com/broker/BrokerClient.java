package pt.com.broker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.caudexorigo.text.StringUtils;

import pt.com.broker.messaging.Acknowledge;
import pt.com.broker.messaging.BrokerListener;
import pt.com.broker.messaging.BrokerMessage;
import pt.com.broker.messaging.CheckStatus;
import pt.com.broker.messaging.DestinationType;
import pt.com.broker.messaging.Enqueue;
import pt.com.broker.messaging.Notify;
import pt.com.broker.messaging.Poll;
import pt.com.broker.messaging.Publish;
import pt.com.broker.messaging.Status;
import pt.com.broker.xml.EndPointReference;
import pt.com.broker.xml.SoapEnvelope;

public class BrokerClient
{
	private String _host;
	private int _portNumber;
	private NetworkHandler _netHandler;
	private String _appName;
	private ConcurrentMap<String, BrokerListener> _async_listeners = new ConcurrentHashMap<String, BrokerListener>();
	private final BlockingQueue<BrokerMessage> _bqueue = new LinkedBlockingQueue<BrokerMessage>();
	private final BlockingQueue<Status> _bstatus = new LinkedBlockingQueue<Status>();

	private ConcurrentMap<String, BlockingQueue<BrokerMessage>> _sync_consumers = new ConcurrentHashMap<String, BlockingQueue<BrokerMessage>>();

	private final Object mutex = new Object();

	public BrokerClient(String host, int portNumber, String appName)
	{
		_host = host;
		_portNumber = portNumber;
		_appName = appName;
		_netHandler = new NetworkHandler(this);
	}

	public void acknowledge(BrokerMessage brkmsg)
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

	public void addAsyncConsumer(Notify notify, BrokerListener listener) throws Exception
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

			String action = "";
			if (notify.destinationType == DestinationType.QUEUE)
			{
				action = "http://services.sapo.pt/broker/listen";
			}
			else if (notify.destinationType == DestinationType.TOPIC)
			{
				action = "http://services.sapo.pt/broker/subscribe";
			}
			else
			{
				throw new IllegalArgumentException("Mal-formed Notification request");
			}
			
			System.out.println("BrokerClient.addAsyncConsumer(): " + action);

			SoapEnvelope soap = buildSoapEnvelope(action);
			soap.body.notify = notify;
			_netHandler.sendMessage(soap);
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Notification request");
		}
	}

	private SoapEnvelope buildSoapEnvelope(String action)
	{
		SoapEnvelope soap = new SoapEnvelope();
		soap.header.wsaAction = action;
		soap.header.wsaFrom = new EndPointReference();
		soap.header.wsaFrom.address = _appName;
		return soap;
	}

	public void close()
	{
		_netHandler.close();
	}

	public void enqueueMessage(BrokerMessage brkmsg) throws Exception
	{
		if ((brkmsg != null) && (StringUtils.isNotBlank(brkmsg.destinationName)))
		{
			Enqueue enqreq = new Enqueue();
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

	protected void feedSyncConsumer(BrokerMessage msg) throws Exception
	{
		_bqueue.offer(msg);
	}
	
	protected void feedStatusConsumer(Status status) throws Exception
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

	public void notifyListener(BrokerMessage msg)
	{
		BrokerListener listener = _async_listeners.get(msg.destinationName);
		listener.onMessage(msg);
		if (listener.isAutoAck())
		{
			acknowledge(msg);
		}
	}

	public BrokerMessage poll(Poll p) throws Exception
	{
		if ((p != null) && (StringUtils.isNotBlank(p.destinationName)))
		{
			SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/poll");
			soap.body.poll = p;
			_netHandler.sendMessage(soap, true);
			return _bqueue.take();
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Poll request");
		}
	}
	
	public Status status(CheckStatus checkstatus) throws Exception
	{
		if ((checkstatus != null))
		{
			SoapEnvelope soap = buildSoapEnvelope("http://services.sapo.pt/broker/checkstatus");
			soap.body.checkStatus = checkstatus;
			_netHandler.sendMessage(soap, true);
			return _bstatus.take();
		}
		else
		{
			throw new IllegalArgumentException("Mal-formed Poll request");
		}
	}

	public void publishMessage(BrokerMessage brkmsg) throws Exception
	{
		if ((brkmsg != null) && (StringUtils.isNotBlank(brkmsg.destinationName)))
		{
			Publish pubreq = new Publish();
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

}
