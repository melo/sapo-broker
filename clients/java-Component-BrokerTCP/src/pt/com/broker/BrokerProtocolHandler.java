package pt.com.broker;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoSession;
import org.caudexorigo.ErrorAnalyser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.net.IoSessionHelper;
import pt.com.broker.xml.SoapEnvelope;

public class BrokerProtocolHandler implements IoHandler
{
	private static Logger log = LoggerFactory.getLogger(BrokerProtocolHandler.class);

	private final NetworkHandler _netHandler;

	public BrokerProtocolHandler(NetworkHandler netHandler)
	{
		super();
		_netHandler = netHandler;
	}

	@Override
	public void exceptionCaught(IoSession ioSession, Throwable error) throws Exception
	{
		Throwable rootCause = ErrorAnalyser.findRootCause(error);
		log.error("Exception Caught:{}, {}", IoSessionHelper.getRemoteAddress(ioSession), rootCause.getMessage());
		if (ioSession.isConnected() && !ioSession.isClosing())
		{
			log.error("STACKTRACE", rootCause);
		}
		ioSession.close();

	}

	@Override
	public void messageReceived(IoSession ioSession, Object message) throws Exception
	{
		if (!(message instanceof SoapEnvelope))
		{
			return;
		}

		final SoapEnvelope request = (SoapEnvelope) message;

		_netHandler.handleReceivedMessage(ioSession, request);
	}

	@Override
	public void messageSent(IoSession ioSession, Object message) throws Exception
	{
		if (log.isDebugEnabled())
		{
			log.debug("Message Sent: '{}', '{}'", IoSessionHelper.getRemoteAddress(ioSession), message.toString());
		}
	}

	@Override
	public void sessionClosed(IoSession ioSession) throws Exception
	{
		log.info("Session Closed: '{}'", IoSessionHelper.getRemoteAddress(ioSession));
		BrokerClientExecutor.schedule(new Connect(_netHandler, (SocketAddress) IoSessionHelper.getRemoteInetAddress(ioSession)), 5000, TimeUnit.MILLISECONDS);

	}

	@Override
	public void sessionCreated(IoSession iosession) throws Exception
	{
		IoSessionHelper.tagWithRemoteAddress(iosession);
		if (log.isDebugEnabled())
		{
			log.debug("Session Created: '{}'", IoSessionHelper.getRemoteAddress(iosession));
		}
	}

	@Override
	public void sessionIdle(IoSession iosession, IdleStatus status) throws Exception
	{
		if (log.isDebugEnabled())
		{
			log.debug("Session Idle:'{}'", IoSessionHelper.getRemoteAddress(iosession));
		}
	}

	@Override
	public void sessionOpened(IoSession iosession) throws Exception
	{
		log.info("Session Opened: '{}'", IoSessionHelper.getRemoteAddress(iosession));
	}

}
