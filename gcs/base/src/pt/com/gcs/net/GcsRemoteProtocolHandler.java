package pt.com.gcs.net;

import java.net.SocketAddress;
import java.util.Set;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.caudexorigo.lang.ErrorAnalyser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Gcs;
import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.messaging.AckMode;
import pt.com.gcs.messaging.LocalQueueConsumers;
import pt.com.gcs.messaging.LocalTopicConsumers;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageType;
import pt.com.gcs.tasks.GcsExecutor;

public class GcsRemoteProtocolHandler extends IoHandlerAdapter
{
	private static Logger log = LoggerFactory.getLogger(GcsRemoteProtocolHandler.class);

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
	public void messageReceived(final IoSession iosession, Object omessage) throws Exception
	{
		final Message msg = (Message) omessage;
		
		

		if (log.isDebugEnabled())
		{
			log.debug("messageReceived() from: {}", getRemoteAddress(iosession));
			log.debug("messageReceived.Type: " + msg.getType());
		}
		
		

		if (msg.getType() == (MessageType.COM_TOPIC))
		{
			LocalTopicConsumers.notify(msg);
		}
		else if (msg.getType() == (MessageType.COM_QUEUE))
		{

			// if (!receivedMessages.isDuplicate(msg.getMessageId()))
			// {
			// LocalQueueConsumers.notify(msg);
			// if (msg.getAcknowledgementMode() == AckMode.AUTO)
			// {
			// LocalQueueConsumers.broadCastAcknowledgement(msg, iosession);
			// }
			// }

			LocalQueueConsumers.notify(msg);

//			final Runnable ack = new Runnable()
//			{
//				public void run()
//				{
//					LocalQueueConsumers.acknowledgeMessage(msg, iosession);
//				}
//			};
//			GcsExecutor.execute(ack);
			
			LocalQueueConsumers.acknowledgeMessage(msg, iosession);
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
			log.debug("messageSent():{}, {}", getRemoteAddress(iosession), message.toString());
		}
	}

	@Override
	public void sessionClosed(final IoSession iosession) throws Exception
	{
		log.debug("sessionClosed():{}", getRemoteAddress(iosession));
		Gcs.connect((SocketAddress) iosession.getAttribute("REMOTE_ADDRESS"));
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
		sayHello(iosession);
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

	public void sayHello(IoSession iosession)
	{
		log.debug("sayHello():{}", getRemoteAddress(iosession));
		
		Message m = new Message();
		String agentId = AgentInfo.getAgentName() + "@" + AgentInfo.getAgentHost() + ":" + AgentInfo.getAgentPort();
		m.setType((MessageType.HELLO));
		m.setDestination("HELLO");
		m.setContent(agentId);

		if (log.isInfoEnabled())
		{
			log.info("Send agentId: " + agentId);
		}

		//iosession.write(m).awaitUninterruptibly();
		iosession.write(m);

		Set<String> topicNameSet = LocalTopicConsumers.getTopicNameSet();
		for (String topicName : topicNameSet)
		{
			LocalTopicConsumers.broadCastTopicInfo(topicName, "CREATE", iosession);
		}

		Set<String> queueNameSet = LocalQueueConsumers.getQueueNameSet();
		for (String queueName : queueNameSet)
		{
			LocalQueueConsumers.broadCastQueueInfo(queueName, "CREATE", iosession);
		}
	}
}
