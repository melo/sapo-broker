package pt.com.manta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.mina.common.IoFilter;
import org.apache.mina.common.IoFilterChain;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.ReadThrottleFilterBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.SocketSessionConfig;
import org.apache.mina.util.SessionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.BrokerConsumer;
import pt.com.broker.BrokerMessage;
import pt.com.broker.BrokerProducer;
import pt.com.broker.DenqueueResultWrapper;
import pt.com.broker.MQ;
import pt.com.broker.Notify;
import pt.com.broker.Publish;
import pt.com.broker.SessionConsumer;
import pt.com.broker.TopicToQueueDispatcher;
import pt.com.codec.SoapCodecFactory;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public class MantaJmsProtocolHandler extends IoHandlerAdapter
{
	private static final Logger log = LoggerFactory.getLogger(MantaJmsProtocolHandler.class);

	private static IoFilter CODEC_FILTER = new ProtocolCodecFilter(new SoapCodecFactory());

	private static final BrokerProducer _brokerProducer = BrokerProducer.getInstance();

	private static final BrokerConsumer _brokerConsumer = BrokerConsumer.getInstance();

	public MantaJmsProtocolHandler()
	{
	}

	@Override
	public void sessionCreated(IoSession iosession) throws Exception
	{
		IoFilterChain chain = iosession.getFilterChain();
		chain.addLast("soap_codec", CODEC_FILTER);

		int buf_size = ((SocketSessionConfig) iosession.getConfig()).getReceiveBufferSize();

		ReadThrottleFilterBuilder filter = new ReadThrottleFilterBuilder();
		filter.setMaximumConnectionBufferSize(buf_size);
		filter.attach(chain);

		SessionUtil.initialize(iosession);

		if (log.isDebugEnabled())
		{
			log.debug("ReceiveBufferSize: " + buf_size);
			log.debug("Session created: " + iosession.getRemoteAddress());
		}

		iosession.setAttribute(MQ.ASYNC_QUEUE_CONSUMER_LIST_ATTR, new ArrayList());
		iosession.setAttribute(MQ.SYNC_QUEUE_CONSUMER_LIST_ATTR, new ArrayList());
	}

	@Override
	public void sessionClosed(IoSession iosession)
	{
		try
		{
			_brokerConsumer.closeSession(iosession);
			log.info("Session closed: " + iosession.getRemoteAddress());
		}
		catch (Throwable e)
		{
			exceptionCaught(iosession, e);
		}
	}

	public void exceptionCaught(IoSession iosession, Throwable cause)
	{
		ErrorHandler.WTF wtf = ErrorHandler.buildSoapFault(cause);
		byte[] ex_msg = wtf.Message;

		iosession.write(ex_msg);
		iosession.close();

		if (!(wtf.Cause instanceof IOException))
		{
			String msg = "Client: " + iosession.getRemoteAddress().toString() + " message: " + wtf.Cause.getMessage();

			if (log.isDebugEnabled())
			{
				log.error(msg, wtf.Cause);
			}
			else
			{
				log.error(msg);
			}
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

		synchronized (session)
		{
			try
			{
				session.suspendRead();
				handleMessage(session, request);
			}
			catch (Throwable e)
			{
				exceptionCaught(session, e);
			}
			finally
			{
				session.resumeRead();
			}
		}
	}

	private void handleMessage(IoSession session, final SoapEnvelope request) throws Throwable
	{
		final String requestSource = MQ.requestSource(request);

		if (request.body.notify != null)
		{
			Notify sb = request.body.notify;

			if (sb.destinationType.equals("TOPIC"))
			{
				SessionConsumer sc = new SessionConsumer(session, requestSource);
				_brokerConsumer.subscribe(sb, sc);
			}
			else if (sb.destinationType.equals("QUEUE"))
			{
				SessionConsumer sc = new SessionConsumer(session, requestSource);
				_brokerConsumer.listen(sb, sc);
			}
			else if (sb.destinationType.equals("TOPIC_AS_QUEUE"))
			{
				TopicToQueueDispatcher.validateDestinationName(sb.destinationName);

				SessionConsumer sc = new SessionConsumer(session, requestSource);
				Publish pubRequest = new Publish();
				pubRequest.brokerMessage.destinationName = MQ.DISPATCHER_TOPIC;
				pubRequest.brokerMessage.textPayload = sb.destinationName;
				_brokerProducer.publishMessage(pubRequest, requestSource);

				_brokerConsumer.listen(sb, sc);
			}
			return;
		}
		else if (request.body.publish != null)
		{
			_brokerProducer.publishMessage(request.body.publish, requestSource);
			return;
		}
		else if (request.body.enqueue != null)
		{
			_brokerProducer.enqueueMessage(request.body.enqueue, requestSource);
			return;
		}
		else if (request.body.denqueue != null)
		{
			DenqueueResultWrapper drw = _brokerConsumer.denqueueMessage(request.body.denqueue, session);
			BrokerMessage bkrmsg = drw.dresult.brokerMessage;

			SoapEnvelope ret_env = new SoapEnvelope();
			ret_env.body.denqueueResponse = drw.dresult;

			SoapHeader soap_header = new SoapHeader();
			EndPointReference epr = new EndPointReference();
			epr.address = requestSource;
			soap_header.wsaFrom = epr;
			soap_header.wsaMessageID = "http://services.sapo.pt/broker/message/" + bkrmsg.messageId;
			soap_header.wsaAction = "http://services.sapo.pt/broker/denqueueresult";
			ret_env.header = soap_header;

			ByteArrayOutputStream holder = new ByteArrayOutputStream(512);

			SoapSerializer.ToXml(ret_env, holder);
			session.write(holder.toByteArray());
			return;
		}
		else if (request.body.acknowledge != null)
		{
			_brokerConsumer.acknowledge(request.body.acknowledge);
			return;
		}
		else
		{
			throw new RuntimeException("Not a valid request");
		}
	}
}
