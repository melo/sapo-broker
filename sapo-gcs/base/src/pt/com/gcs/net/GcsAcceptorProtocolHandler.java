package pt.com.gcs.net;

import java.net.SocketAddress;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.caudexorigo.lang.ErrorAnalyser;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Gcs;
import pt.com.gcs.messaging.AckMode;
import pt.com.gcs.messaging.LocalQueueConsumers;
import pt.com.gcs.messaging.LocalTopicConsumers;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageType;
import pt.com.gcs.messaging.ReceivedMessages;

public class GcsAcceptorProtocolHandler extends IoHandlerAdapter
{
	private static Logger log = LoggerFactory.getLogger(GcsAcceptorProtocolHandler.class);


	private static ReceivedMessages receivedMessages = new ReceivedMessages();

	@Override
	public void exceptionCaught(IoSession iosession, Throwable cause) throws Exception
	{
		Throwable rootCause = ErrorAnalyser.findRootCause(cause);
		log.error("Exception Caught:{}, {}", getRemoteAddress(iosession), rootCause.getMessage());
		if (iosession.isConnected() && !iosession.isClosing())
		{
			log.error("STACKTRACE", rootCause);
		}
	}

	@Override
	public void messageReceived(IoSession iosession, Object message) throws Exception
	{
		// System.out.println("GcsAcceptorProtocolHandler.messageReceived.toString:
		// " + message.toString());

		if (log.isDebugEnabled())
		{
			log.debug("GcsAcceptorProtocolHandler.messageReceived() from: {}", getRemoteAddress(iosession));
		}

		Message msg = (Message) message;
		if (msg.getType() == (MessageType.COM_TOPIC))
		{
			LocalTopicConsumers.notify(msg);
		}
		else if (msg.getType() == (MessageType.COM_QUEUE))
		{
			// System.out.println("GcsAcceptorProtocolHandler.messageReceived().MessageId:
			// " + msg.getMessageId());
// if (!receivedMessages.isDuplicate(msg.getMessageId()))
// {
// LocalQueueConsumers.notify(msg);
// if (msg.getAcknowledgementMode() == AckMode.AUTO)
// {
// LocalQueueConsumers.broadCastAcknowledgement(msg, iosession);
// }
// }

			LocalQueueConsumers.notify(msg);
			// System.out.println("GcsAcceptorProtocolHandler.messageReceived().getReadMessages():
			// " + iosession.getReadMessages());
			if (msg.getAcknowledgementMode() == AckMode.AUTO)
			{
				LocalQueueConsumers.broadCastAcknowledgement(msg, iosession);
			}
		}
		else if (msg.getType() == (MessageType.HELLO))
		{
			validatePeer(iosession, msg.getContent());
			boolean isValid = ((Boolean) iosession.getAttribute("GcsRemoteProtocolHandler.ISVALID")).booleanValue();
			if (isValid)
			{
				String paddr = String.valueOf(iosession.getAttribute("GcsAcceptorProtocolHandler.PEER_ADDRESS"));
				log.warn("A peer from \"{}\" tried to connect but it does not appear in the world map.", paddr);				
				iosession.close().awaitUninterruptibly();
			}
			else
			{
				System.out.println("Peer is valid!");
				return;
			}
			return;
		}
		else
		{
			log.error("GcsAcceptorProtocolHandler.messageReceived():  Don't know how to handle message");
		}
	}

	@Override
	public void messageSent(IoSession iosession, Object message) throws Exception
	{
		if (log.isDebugEnabled())
		{
			log.debug("messageSent():{}, {}", getRemoteAddress(iosession), message.toString());
		}
	}

	@Override
	public void sessionClosed(IoSession iosession) throws Exception
	{
		log.debug("sessionClosed():{}", getRemoteAddress(iosession));
	}

	@Override
	public void sessionCreated(IoSession iosession) throws Exception
	{
		log.debug("sessionCreated():{}", getRemoteAddress(iosession));
		iosession.setAttribute("REMOTE_ADDRESS", iosession.getRemoteAddress());
	}

	@Override
	public void sessionIdle(IoSession iosession, IdleStatus status) throws Exception
	{
		 log.debug("sessionIdle():{}", getRemoteAddress(iosession));
	}

	@Override
	public void sessionOpened(IoSession iosession) throws Exception
	{

		log.debug("sessionOpened():{}", getRemoteAddress(iosession));

	}

	
	private void validatePeer(IoSession iosession, String helloMessage)
	{
		try
		{
			String peerName = StringUtils.substringBefore(helloMessage, "@");
			String peerAddr = StringUtils.substringAfter(helloMessage, "@");
			String peerHost = StringUtils.substringBefore(peerAddr, ":");
			int peerPort = Integer.parseInt(StringUtils.substringAfter(peerAddr, ":"));
			iosession.setAttribute("GcsAcceptorProtocolHandler.PEER_ADDRESS", peerAddr);
			Peer peer = new Peer(peerName, peerHost, peerPort);
			if (Gcs.getPeerList().contains(peer))
			{
				iosession.setAttribute("GcsAcceptorProtocolHandler.ISVALID", true);
				return;
			}
		}
		catch (Throwable t)
		{
			iosession.setAttribute("GcsAcceptorProtocolHandler.PEER_ADDRESS", "Unknown address");
			log.error(t.getMessage(), t);
		}

		iosession.setAttribute("GcsAcceptorProtocolHandler.ISVALID", false);
	}

	private String getRemoteAddress(IoSession iosession)
	{
		String remoteServer;
		try
		{
			remoteServer = ((SocketAddress) iosession.getAttribute("REMOTE_ADDRESS")).toString();
		}
		catch (Throwable e)
		{
			remoteServer = "Can't determine server address";
		}
		return remoteServer;
	}
}
