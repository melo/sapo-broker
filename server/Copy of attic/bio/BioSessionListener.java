package pt.com.manta.bio;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Date;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.BrokerMessage;
import pt.com.broker.Notification;
import pt.com.text.DateUtil;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapSerializer;

public class BioSessionListener implements MessageListener
{
	private static final Logger log = LoggerFactory.getLogger(BioSessionListener.class);

	private Socket _socket;

	private DataOutputStream out;

	private int _ackMode;

	private BioBroker _broker;
	
	private String _consumerName;

	public BioSessionListener(BioBroker broker, int ackMode, String consumerName)
	{
		super();
		_broker = broker;
		_socket = broker.getSocket();
		_ackMode = ackMode;
		_consumerName = consumerName;
		try
		{
			out = new DataOutputStream(_socket.getOutputStream());
		}
		catch (IOException e)
		{
			log.error(e.getMessage(), e);
		}


	}

	public void onMessage(Message amsg)
	{
		TextMessage msg;
		if (amsg instanceof TextMessage)
		{
			msg = (TextMessage) amsg;
		}
		else
		{
			return;
		}

		Notification nt = new Notification();
		BrokerMessage bkrm = new BrokerMessage();
		try
		{
			bkrm.correlationId = msg.getJMSCorrelationID();
			bkrm.deliveryMode = msg.getJMSDeliveryMode();
			bkrm.destinationName = msg.getJMSDestination().toString();
			bkrm.expiration = DateUtil.formatISODate(new Date(msg.getJMSExpiration()));
			bkrm.messageId = msg.getJMSMessageID();
			bkrm.priority = msg.getJMSPriority();
			bkrm.textPayload = msg.getText();
			nt.brokerMessage = bkrm;

			SoapEnvelope soap_env = new SoapEnvelope();
			soap_env.body.notification = nt; 
			
			ByteArrayOutputStream holder = new ByteArrayOutputStream(512);

			SoapSerializer.ToXml(soap_env, holder);	
			
			
			out.writeInt(holder.size());
			out.write(holder.toByteArray());
			out.flush();

			if (_ackMode == Session.CLIENT_ACKNOWLEDGE)
			{
				_broker.getWaitingAckMessages().put(msg.getJMSMessageID(), msg);
			}
		}
		catch (SocketException e)
		{
			_broker.removeConsumer(_consumerName);
		}
		catch (Exception e)
		{
			log.error(e.getMessage(), e);
		}
		
		
	}
}
