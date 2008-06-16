package pt.com.gcs.messaging;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.net.IoSessionHelper;
import pt.com.gcs.net.Peer;

class GcsAcceptorProtocolHandler extends IoHandlerAdapter
{
	private static Logger log = LoggerFactory.getLogger(GcsAcceptorProtocolHandler.class);

	@Override
	public void exceptionCaught(IoSession iosession, Throwable cause) throws Exception
	{
		Throwable rootCause = ErrorAnalyser.findRootCause(cause);
		log.error("Exception Caught:'{}', '{}'", IoSessionHelper.getRemoteAddress(iosession), rootCause.getMessage());
		if (iosession.isConnected() && !iosession.isClosing())
		{
			log.error("STACKTRACE", rootCause);
		}
	}

	@Override
	public void messageReceived(IoSession iosession, Object message) throws Exception
	{
		final Message msg = (Message) message;

		if (log.isDebugEnabled())
		{
			log.debug("Message Received from: '{}', Type: '{}'", IoSessionHelper.getRemoteAddress(iosession), msg.getType());
		}

		if (msg.getType() == MessageType.ACK)
		{

			Gcs.ackMessage(msg.getDestination(), msg.getMessageId());

			return;
		}
		else if (msg.getType() == (MessageType.HELLO))
		{
			validatePeer(iosession, msg.getContent());
			boolean isValid = ((Boolean) iosession.getAttribute("GcsAcceptorProtocolHandler.ISVALID")).booleanValue();
			if (!isValid)
			{
				String paddr = String.valueOf(iosession.getAttribute("GcsAcceptorProtocolHandler.PEER_ADDRESS"));
				log.warn("A peer from \"{}\" tried to connect but it does not appear in the world map.", paddr);
				iosession.close().awaitUninterruptibly();
			}
			else
			{
				log.debug("Peer is valid!");
				return;
			}
			return;
		}
		else if ((msg.getType() == MessageType.SYSTEM_TOPIC) || (msg.getType() == MessageType.SYSTEM_QUEUE))
		{
			String payload = msg.getContent();

			final String action = extract(payload, "<action>", "</action>");
			// final String src_name = extract(payload, "<source-name>", "</source-name>");
			// final String src_ip = extract(payload, "<source-ip>", "</source-ip>");
			final String destinationName = extract(payload, "<destination>", "</destination>");

			log.info("Action: '{}' Consumer. Destination: '{}'", action, destinationName);

			if (msg.getType() == MessageType.SYSTEM_TOPIC)
			{
				if (action.equals("CREATE"))
				{
					RemoteTopicConsumers.add(msg.getDestination(), iosession);
				}
				else if (action.equals("DELETE"))
				{
					RemoteTopicConsumers.remove(msg.getDestination(), iosession);
				}
			}
			else if (msg.getType() == MessageType.SYSTEM_QUEUE)
			{
				if (action.equals("CREATE"))
				{

					if (StringUtils.contains(msg.getDestination(), "@"))
					{
						DispatcherList.create(msg.getDestination());
					}

					RemoteQueueConsumers.add(msg.getDestination(), iosession);
					QueueProcessorList.get(destinationName);
				}
				else if (action.equals("DELETE"))
				{
					RemoteQueueConsumers.remove(msg.getDestination(), iosession);
				}
			}
		}
		else
		{
			log.warn("Unkwown message type. Don't know how to handle message");
		}
	}

	@Override
	public void messageSent(IoSession iosession, Object message) throws Exception
	{
		if (log.isDebugEnabled())
		{
			log.debug("Message Sent: '{}', '{}'", IoSessionHelper.getRemoteAddress(iosession), message.toString());
		}
	}

	@Override
	public void sessionClosed(IoSession iosession) throws Exception
	{
		log.info("Session Closed: '{}'", IoSessionHelper.getRemoteAddress(iosession));
		RemoteTopicConsumers.remove(iosession);
		RemoteQueueConsumers.remove(iosession);
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

	private void validatePeer(IoSession iosession, String helloMessage)
	{
		log.debug("\"Hello\" message received: '{}'", helloMessage);
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
				log.debug("Peer '{}' exists in the world map'", peer.toString());
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

	private String extract(String ins, String prefix, String sufix)
	{
		if (StringUtils.isBlank(ins))
		{
			return "";
		}

		int s = ins.indexOf(prefix) + prefix.length();
		int e = ins.indexOf(sufix);
		return ins.substring(s, e);
	}
}
