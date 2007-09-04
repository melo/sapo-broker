package pt.com.gcs.net;

import java.net.SocketAddress;
import java.util.Set;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.caudexorigo.lang.ErrorAnalyser;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Gcs;
import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.messaging.LocalQueueConsumers;
import pt.com.gcs.messaging.LocalTopicConsumers;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageType;
import pt.com.gcs.messaging.QueueProcessorList;
import pt.com.gcs.messaging.RemoteQueueConsumers;
import pt.com.gcs.messaging.RemoteTopicConsumers;

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
		if (log.isDebugEnabled())
		{
			log.debug("GcsRemoteProtocolHandler.messageReceived() from: {}", getRemoteAddress(iosession));
		}

		final Message msg = (Message) omessage;

		if (msg.getType() == MessageType.ACK)
		{
			QueueProcessorList.get(msg.getDestination()).ack(msg);
			return;
		}

		String payload = msg.getContent();
		log.info(payload);
		final String action = extract(payload, "<action>", "</action>");
// final String src_name = extract(payload, "<source-name>", "</source-name>");
// final String src_ip = extract(payload, "<source-ip>", "</source-ip>");
		final String destinationName = extract(payload, "<destination>", "</destination>");

		if (msg.getType() == MessageType.SYSTEM_TOPIC)
		{
			if (action.equals("CREATE"))
			{
				System.out.println("GcsRemoteProtocolHandler.CREATE: " + msg.getDestination());
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
				RemoteQueueConsumers.add(msg.getDestination(), iosession);
				QueueProcessorList.get(destinationName).wakeup();

			}
			else if (action.equals("DELETE"))
			{
				RemoteQueueConsumers.remove(msg.getDestination(), iosession);
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
			log.debug("messageSent():{}, {}", getRemoteAddress(iosession), message.toString());
		}
	}

	@Override
	public void sessionClosed(final IoSession iosession) throws Exception
	{
		log.debug("sessionClosed():{}", getRemoteAddress(iosession));
		RemoteTopicConsumers.remove(iosession);
		RemoteQueueConsumers.remove(iosession);

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


	private static String extract(String ins, String prefix, String sufix)
	{
		if (StringUtils.isBlank(ins))
		{
			return "";
		}

		int s = ins.indexOf(prefix) + prefix.length();
		int e = ins.indexOf(sufix);
		return ins.substring(s, e);
	}

	public void sayHello(IoSession iosession)
	{

		Message m = new Message();
		String agentId = AgentInfo.getAgentName() + "@" + AgentInfo.getAgentHost() + ":" + AgentInfo.getAgentPort();
		m.setType((MessageType.HELLO));
		m.setDestination("HELLO");
		m.setContent(agentId);

		if (log.isDebugEnabled())
		{
			log.debug("Send agentId: " + agentId);
		}

		iosession.write(m).awaitUninterruptibly();

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
