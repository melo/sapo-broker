package pt.com.broker.net;

import java.io.IOException;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.write.WriteTimeoutException;
import org.caudexorigo.concurrent.Sleep;
import org.caudexorigo.io.UnsynchByteArrayOutputStream;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.BrokerExecutor;
import pt.com.broker.core.ErrorHandler;
import pt.com.broker.messaging.Accepted;
import pt.com.broker.messaging.BrokerConsumer;
import pt.com.broker.messaging.BrokerProducer;
import pt.com.broker.messaging.BrokerSyncConsumer;
import pt.com.broker.messaging.MQ;
import pt.com.broker.messaging.Notify;
import pt.com.broker.messaging.Poll;
import pt.com.broker.messaging.QueueSessionListenerList;
import pt.com.broker.messaging.Status;
import pt.com.broker.messaging.TopicSubscriberList;
import pt.com.broker.messaging.Unsubscribe;
import pt.com.broker.xml.FaultCode;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapSerializer;
import pt.com.gcs.conf.GcsInfo;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.net.IoSessionHelper;

public class BrokerProtocolHandler extends IoHandlerAdapter
{
	private static final Logger log = LoggerFactory.getLogger(BrokerProtocolHandler.class);

	private static final BrokerProducer _brokerProducer = BrokerProducer.getInstance();

	private static final BrokerConsumer _brokerConsumer = BrokerConsumer.getInstance();

	private static final int MAX_WRITE_BUFFER_SIZE = 5000;

	public BrokerProtocolHandler()
	{
	}

	@Override
	public void sessionCreated(IoSession iosession) throws Exception
	{
		IoSessionHelper.tagWithRemoteAddress(iosession);
		if (log.isDebugEnabled())
		{
			log.debug("Session created: " + IoSessionHelper.getRemoteAddress(iosession));
		}
	}

	@Override
	public void sessionClosed(IoSession iosession)
	{
		try
		{
			String remoteClient = IoSessionHelper.getRemoteAddress(iosession);
			log.info("Session closed: " + remoteClient);
			QueueSessionListenerList.removeSession(iosession);
			TopicSubscriberList.removeSession(iosession);
		}
		catch (Throwable e)
		{
			exceptionCaught(iosession, e);
		}
	}

	public void exceptionCaught(IoSession iosession, Throwable cause)
	{
		exceptionCaught(iosession, cause, null);
	}

	public void exceptionCaught(IoSession iosession, Throwable cause, String actionId)
	{
		ErrorHandler.WTF wtf = ErrorHandler.buildSoapFault(cause);
		SoapEnvelope ex_msg = wtf.Message;
		if (actionId != null)
		{
			ex_msg.body.fault.faultCode.subcode = new FaultCode();
			ex_msg.body.fault.faultCode.subcode.value = "action-id:" + actionId;

		}

		publishFault(ex_msg);

		String client = IoSessionHelper.getRemoteAddress(iosession);

		if (!(wtf.Cause instanceof IOException))
		{
			try
			{
				iosession.write(ex_msg);
			}
			catch (Throwable t)
			{
				log.error("The error information could not be delivered to the client", t);
			}
		}

		try
		{
			iosession.close();
		}
		catch (Throwable t)
		{
			log.error("Error closing client connection", t);
		}

		try
		{
			String msg = "";

			if (wtf.Cause instanceof WriteTimeoutException)
			{
				String emsg = "Connection was closed because client was too slow! Slow queue consumers should use polling.";
				msg = "Client: " + client + ". Message: " + emsg;
			}
			else
			{
				String emsg = wtf.Cause.getMessage();
				msg = "Client: " + client + ". Message: " + emsg;
			}

			if (wtf.Cause instanceof IOException)
			{
				log.error(msg);
			}
			else
			{
				log.error(msg, wtf.Cause);
			}

		}
		catch (Throwable t)
		{
			log.error("Unspecified error", t);
		}
	}

	private void publishFault(SoapEnvelope ex_msg)
	{
		try
		{
			UnsynchByteArrayOutputStream out = new UnsynchByteArrayOutputStream();
			SoapSerializer.ToXml(ex_msg, out);
			Message errmsg = new Message();
			errmsg.setContent(new String(out.toByteArray()));
			errmsg.setDestination(String.format("/system/faults/#%s#", GcsInfo.getAgentName()));
			Gcs.publish(errmsg);
		}
		catch (Throwable t)
		{
			log.error(t.getMessage(), t);
		}
	}

	@Override
	public void messageReceived(final IoSession session, Object message) throws Exception
	{
		if (!(message instanceof SoapEnvelope))
		{
			return;
		}

		final SoapEnvelope request = (SoapEnvelope) message;
		handleMessage(session, request);
	}

	private void handleMessage(IoSession session, final SoapEnvelope request)
	{
		String actionId = null;

		try
		{
			final String requestSource = MQ.requestSource(request);

			if (request.body.notify != null)
			{
				Notify sb = request.body.notify;
				actionId = sb.actionId;

				if (StringUtils.isBlank(sb.destinationName))
				{
					throw new IllegalArgumentException("Must provide a valid destination");
				}

				if (sb.destinationType.equals("TOPIC"))
				{
					_brokerConsumer.subscribe(sb, session);
				}
				else if (sb.destinationType.equals("QUEUE"))
				{
					_brokerConsumer.listen(sb, session);
				}
				else if (sb.destinationType.equals("TOPIC_AS_QUEUE"))
				{
					if (StringUtils.contains(sb.destinationName, "@"))
					{
						_brokerConsumer.listen(sb, session);
					}
					else
					{
						throw new IllegalArgumentException("Not a valid destination name for a TOPIC_AS_QUEUE consumer");
					}
				}
				sendAccepted(session, actionId);
				return;
			}
			else if (request.body.publish != null)
			{
				actionId = request.body.publish.actionId;
				_brokerProducer.publishMessage(request.body.publish, requestSource);
				sendAccepted(session, actionId);
				return;
			}
			else if (request.body.enqueue != null)
			{
				actionId = request.body.enqueue.actionId;
				_brokerProducer.enqueueMessage(request.body.enqueue, requestSource);
				sendAccepted(session, actionId);
				return;
			}
			else if (request.body.poll != null)
			{
				actionId = request.body.poll.actionId;
				sendAccepted(session, actionId);

				Poll poll = request.body.poll;
				BrokerSyncConsumer.poll(poll, session);
				return;
			}
			else if (request.body.acknowledge != null)
			{
				_brokerProducer.acknowledge(request.body.acknowledge);
				return;
			}
			else if (request.body.accepted != null)
			{
				// TODO: deal with the ack
				return;
			}
			else if (request.body.unsubscribe != null)
			{
				Unsubscribe unsubs = request.body.unsubscribe;
				_brokerConsumer.unsubscribe(unsubs, session);
				actionId = request.body.unsubscribe.actionId;
				sendAccepted(session, actionId);
				return;
			}
			else if (request.body.checkStatus != null)
			{
				SoapEnvelope soap_status = new SoapEnvelope();
				soap_status.body.status = new Status();
				session.write(soap_status);
				return;
			}
			else
			{
				throw new RuntimeException("Not a valid request");
			}
		}
		catch (Throwable t)
		{
			exceptionCaught(session, t, actionId);
		}
	}

	private void sendAccepted(final IoSession ios, final String actionId)
	{
		if (actionId != null)
		{
			SoapEnvelope soap_a = new SoapEnvelope();
			Accepted a = new Accepted();
			a.actionId = actionId;
			soap_a.body.accepted = a;
			ios.write(soap_a);

			boolean isSuspended = (Boolean) ios.getAttribute("IS_SUSPENDED", Boolean.FALSE);

			if ((ios.getScheduledWriteMessages() > MAX_WRITE_BUFFER_SIZE) && !isSuspended)
			{
				ios.suspendRead();
				ios.setAttribute("IS_SUSPENDED", Boolean.TRUE);

				Runnable resumer = new Runnable()
				{
					public void run()
					{
						int counter = 0;
						while (true)
						{
							Sleep.time(5);
							counter++;
							if (ios.getScheduledWriteMessages() <= MAX_WRITE_BUFFER_SIZE)
							{
								ios.resumeRead();
								ios.setAttribute("IS_SUSPENDED", Boolean.FALSE);
								return;
							}
							if (counter % 1000 == 0)
							{
								log.warn("Client is slow to read ack messages.");
							}
						}
					}
				};
				BrokerExecutor.execute(resumer);
			}
		}

	}
}
