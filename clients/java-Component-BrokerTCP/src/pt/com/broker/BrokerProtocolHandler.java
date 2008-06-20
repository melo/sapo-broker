package pt.com.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import org.caudexorigo.io.UnsynchByteArrayInputStream;
import org.caudexorigo.io.UnsynchByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.messaging.BrokerMessage;
import pt.com.broker.messaging.Status;
import pt.com.broker.net.ProtocolHandler;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapFault;
import pt.com.broker.xml.SoapSerializer;

public class BrokerProtocolHandler extends ProtocolHandler<SoapEnvelope>
{
	private static final Logger log = LoggerFactory.getLogger(BrokerProtocolHandler.class);

	private final BrokerClient _brokerClient;

	private final NetworkConnector _connector;

	public BrokerProtocolHandler(BrokerClient brokerClient) throws UnknownHostException, IOException
	{
		_brokerClient = brokerClient;

		_connector = new NetworkConnector(brokerClient.getHost(), brokerClient.getPort());
	}

	public SoapEnvelope unmarshal(byte[] message)
	{
		UnsynchByteArrayInputStream bin = new UnsynchByteArrayInputStream(message);
		SoapEnvelope msg = SoapSerializer.FromXml(bin);
		return msg;
	}

	@Override
	public NetworkConnector getConnector()
	{
		return _connector;
	}

	@Override
	public void onConnectionClose()
	{
		log.debug("Connection Closed");
		
	}

	@Override
	public void onConnectionOpen()
	{
		log.debug("Connection Opened");
		try
		{
			_brokerClient.sendSubscriptions();
		}
		catch (Throwable t)
		{
			log.error(t.getMessage(), t);
		}		
	}

	@Override
	public void onError(Throwable error)
	{
		log.error(error.getMessage(), error);		
	}

	@Override
	protected void handleReceivedMessage(SoapEnvelope request)
	{
		if (request.body.notification != null)
		{
			BrokerMessage msg = request.body.notification.brokerMessage;

			if (msg != null)
			{
				SyncConsumer sc = SyncConsumerList.get(msg.destinationName);
				if (sc.count() > 0)
				{
					sc.offer(msg);
					sc.decrement();
				}
				else
				{
					_brokerClient.notifyListener(msg);
				}
			}
		}
		else if (request.body.status != null)
		{
			Status status = request.body.status;
			try
			{
				_brokerClient.feedStatusConsumer(status);
			}
			catch (Throwable t)
			{
				log.error(t.getMessage(), t);
			}
		}
		else if (request.body.fault != null)
		{
			SoapFault fault = request.body.fault;
			log.error(fault.toString());
			throw new RuntimeException(fault.faultReason.text);
		}
	}

	@Override
	public SoapEnvelope decode(DataInputStream in) throws IOException
	{
		int len = in.readInt();
		byte[] buf = new byte[len];
		in.readFully(buf);
		UnsynchByteArrayInputStream holder = new UnsynchByteArrayInputStream(buf);
		return SoapSerializer.FromXml(holder);
	}

	@Override
	public void encode(SoapEnvelope message, DataOutputStream out) throws IOException
	{
		UnsynchByteArrayOutputStream holder = new UnsynchByteArrayOutputStream();
		SoapSerializer.ToXml(message, holder);
		byte[] buf = holder.toByteArray();
		out.writeInt(buf.length);
		out.write(buf);
	}
}
