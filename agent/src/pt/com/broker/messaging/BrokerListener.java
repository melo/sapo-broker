package pt.com.broker.messaging;

import java.util.Date;

import org.apache.mina.common.IoSession;
import org.caudexorigo.text.DateUtil;

import pt.com.broker.xml.EndPointReference;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapHeader;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageListener;

public abstract class BrokerListener implements MessageListener
{
	protected static SoapEnvelope buildNotification(Message msg)
	{
		Notification nt = new Notification();
		BrokerMessage bkrm = nt.brokerMessage;
		
		bkrm.destinationName = msg.getDestination();
		bkrm.timestamp = DateUtil.formatISODate(new Date(msg.getTimestamp()));
		bkrm.expiration = DateUtil.formatISODate(new Date(msg.getExpiration()));
		bkrm.messageId = msg.getMessageId();		
		bkrm.textPayload = msg.getContent();
		nt.brokerMessage = bkrm;

		SoapEnvelope soap_env = new SoapEnvelope();
		SoapHeader soap_header = new SoapHeader();
		EndPointReference epr = new EndPointReference();
		epr.address = msg.getSourceApp();
		soap_header.wsaFrom = epr;
		soap_header.wsaMessageID = "http://services.sapo.pt/broker/message/" + bkrm.messageId;
		soap_header.wsaAction = "http://services.sapo.pt/broker/notification/";
		soap_env.header = soap_header;

		soap_env.body.notification = nt;

		return soap_env;
	}
	
	public abstract void addConsumer(IoSession iosession);
	
	public abstract void removeConsumer(IoSession iosession);
}
