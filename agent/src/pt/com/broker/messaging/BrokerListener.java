package pt.com.broker.messaging;

import java.util.Date;

import org.caudexorigo.text.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.xml.EndPointReference;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapHeader;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageListener;

public abstract class BrokerListener implements MessageListener
{
	private static final Logger log = LoggerFactory.getLogger(BrokerListener.class);

	private static final long ONE_YEAR = 1000L * 3600L * 24L * 365L;

	public SoapEnvelope buildNotification(Message msg)
	{
		Notification nt = new Notification();
		BrokerMessage bkrm = nt.brokerMessage;

		// String sourceAgent = msg.getStringProperty(MQ.MESSAGE_SOURCE);

		bkrm.correlationId = msg.getCorrelationId();
		// bkrm.deliveryMode = DeliveryMode.lookup(msg.getJMSDeliveryMode());
		bkrm.destinationName = msg.getDestination();
		if (msg.getTtl() == 0)
		{
			// Set the expiration a year from now
			bkrm.expiration = DateUtil.formatISODate(new Date(System.currentTimeMillis() + ONE_YEAR));
		}

		bkrm.messageId = msg.getMessageId();
		bkrm.priority = msg.getPriority();
		bkrm.textPayload = msg.getContent();
		nt.brokerMessage = bkrm;

		SoapEnvelope soap_env = new SoapEnvelope();
		SoapHeader soap_header = new SoapHeader();
		EndPointReference epr = new EndPointReference();
		epr.address = "sourceAgent";
		soap_header.wsaFrom = epr;
		soap_header.wsaMessageID = "http://services.sapo.pt/broker/message/" + bkrm.messageId;
		soap_header.wsaAction = "http://services.sapo.pt/broker/notification/";
		soap_env.header = soap_header;

		soap_env.body.notification = nt;

		return soap_env;
	}
}
