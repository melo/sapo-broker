package pt.com.broker;

import java.io.ByteArrayOutputStream;
import java.util.Date;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.text.DateUtil;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public abstract class BrokerListener implements MessageListener
{
	private static final Logger log = LoggerFactory.getLogger(BrokerListener.class);

	private static final long ONE_YEAR = 1000L * 3600L * 24L * 365L;

	protected TextMessage buildMessage(Message amsg) throws JMSException
	{
		TextMessage msg;
		if (amsg instanceof TextMessage)
		{
			msg = (TextMessage) amsg;
			if (log.isDebugEnabled())
			{
				log.debug("Message received:\n" + msg.getText());
			}
			return msg;
		}
		else
		{
			log.error("Invalid message type. Only JMS-TextMessages are supported. The message will be discarded.");
			return null;
		}
	}

	public byte[] buildNotification(TextMessage msg) throws JMSException
	{
		Notification nt = new Notification();
		BrokerMessage bkrm = nt.brokerMessage;

		String sourceAgent = msg.getStringProperty(MQ.MESSAGE_SOURCE);

		bkrm.correlationId = msg.getJMSCorrelationID();
		bkrm.deliveryMode = DeliveryMode.lookup(msg.getJMSDeliveryMode());
		bkrm.destinationName = msg.getJMSDestination().toString();
		if (msg.getJMSExpiration() == Long.MAX_VALUE)
		{
			// Set the expiration a year from now
			bkrm.expiration = DateUtil.formatISODate(new Date(System.currentTimeMillis() + ONE_YEAR));
		}

		bkrm.messageId = msg.getJMSMessageID();
		bkrm.priority = msg.getJMSPriority();
		bkrm.textPayload = msg.getText();
		nt.brokerMessage = bkrm;

		SoapEnvelope soap_env = new SoapEnvelope();
		SoapHeader soap_header = new SoapHeader();
		EndPointReference epr = new EndPointReference();
		epr.address = sourceAgent;
		soap_header.wsaFrom = epr;
		soap_header.wsaMessageID = "http://services.sapo.pt/broker/message/" + bkrm.messageId;
		soap_header.wsaAction = "http://services.sapo.pt/broker/notification/";
		soap_env.header = soap_header;

		soap_env.body.notification = nt;
		ByteArrayOutputStream out = new ByteArrayOutputStream(512);
		SoapSerializer.ToXml(soap_env, out);
		byte[] response = out.toByteArray();
		return response;
	}
}
