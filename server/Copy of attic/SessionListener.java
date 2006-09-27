package pt.com.broker;

import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.text.DateUtil;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public class SessionListener implements MessageListener
{
	private static final Logger log = LoggerFactory.getLogger(SessionListener.class);

	private final List<IoSession> slisteners = new CopyOnWriteArrayList<IoSession>();

	private static final long ONE_YEAR = 1000L * 3600L * 24L * 365L;

	private final MessageConsumer _consumer;

	public SessionListener(MessageConsumer consumer)
	{
		_consumer = consumer;
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
		BrokerMessage bkrm = nt.brokerMessage;
		try
		{
			String sourceAgent = amsg.getStringProperty("SourceAgent");

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
			epr.address = "broker://agent/" + sourceAgent + "/" + bkrm.destinationName;
			soap_header.wsaFrom = epr;
			soap_header.wsaMessageID = "http://services.sapo.pt/broker/message/" + bkrm.messageId;
			soap_header.wsaAction = "http://services.sapo.pt/broker/notification/";
			soap_env.header = soap_header;

			soap_env.body.notification = nt;
			ByteArrayOutputStream out = new ByteArrayOutputStream(512);
			SoapSerializer.ToXml(soap_env, out);
			byte[] response = out.toByteArray();

			for (IoSession iosession : slisteners)
			{
				try
				{
					Notify notify = (Notify) iosession.getAttribute(bkrm.destinationName);

					iosession.write(response);

					if (notify.acknowledgeMode == AcknowledgeMode.CLIENT)
					{
						WaitingAckMessages.put(msg.getJMSMessageID(), msg);
					}
				}
				catch (Throwable e)
				{
					(iosession.getHandler()).exceptionCaught(iosession, e);
				}
			}
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public void addSession(IoSession iosession, Notify sb)
	{
		iosession.setAttribute(sb.destinationName, sb);
		slisteners.add(iosession);
	}

	public void removeSession(IoSession iosession)
	{
		slisteners.remove(iosession);

		if (slisteners.size() < 1)
		{
			try
			{
				_consumer.close();
				BrokerConsumer.deleteListener(this, iosession);
			}
			catch (Throwable e)
			{
				throw new RuntimeException(e);
			}
		}
	}
}
