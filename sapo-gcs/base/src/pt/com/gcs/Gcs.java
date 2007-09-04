package pt.com.gcs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.traffic.ReadThrottleFilterBuilder;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketConnector;
import org.apache.mina.transport.socket.nio.SocketSessionConfig;
import org.caudexorigo.concurrent.CustomExecutors;
import org.caudexorigo.concurrent.Sleep;
import org.caudexorigo.lang.ErrorAnalyser;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.conf.WorldMap;
import pt.com.gcs.messaging.LocalQueueConsumers;
import pt.com.gcs.messaging.LocalTopicConsumers;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageListener;
import pt.com.gcs.messaging.MessageType;
import pt.com.gcs.messaging.QueueProcessorList;
import pt.com.gcs.messaging.RemoteTopicConsumers;
import pt.com.gcs.messaging.TopicToQueueDispatcher;
import pt.com.gcs.net.GcsAcceptorProtocolHandler;
import pt.com.gcs.net.GcsRemoteProtocolHandler;
import pt.com.gcs.net.Peer;
import pt.com.gcs.net.codec.GcsCodec;
import pt.com.gcs.tasks.Connect;
import pt.com.gcs.tasks.GcsExecutor;

public class Gcs
{
	private static Logger log = LoggerFactory.getLogger(Gcs.class);

	private static final int NCPU = Runtime.getRuntime().availableProcessors();

	private static final int IO_THREADS = NCPU + 1;

	private static final String SERVICE_NAME = "SAPO GCS";

	private static final Gcs instance = new Gcs();

	private SocketAcceptor acceptor;

	private SocketConnector connector;

	private WorldMap _wmap;


	private Gcs()
	{
		log.info("{} STARTING", SERVICE_NAME);
		try
		{
			startAcceptor(AgentInfo.getAgentPort());
			startConnector();
			populateWorldMap();
		}
		catch (Throwable t)
		{
			log.error(ErrorAnalyser.findRootCause(t).getMessage());
			Shutdown.now();
		}
		Sleep.time(AgentInfo.getInitialDelay());
	}

	private void startAcceptor(int portNumber) throws IOException
	{
		acceptor = new SocketAcceptor(IO_THREADS, CustomExecutors.newULimitCachedThreadPool(16));

		acceptor.setReuseAddress(true);
		((SocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);
		((SocketSessionConfig) acceptor.getSessionConfig()).setTcpNoDelay(false);

		acceptor.setBacklog(100);

		acceptor.setLocalAddress(new InetSocketAddress(portNumber));

		DefaultIoFilterChainBuilder filterChainBuilder = acceptor.getFilterChain();

		// Add CPU-bound job first,
		filterChainBuilder.addLast("GCS_CODEC", new ProtocolCodecFilter(new GcsCodec()));
		// and then a thread pool.
		filterChainBuilder.addLast("threadPool", new ExecutorFilter(CustomExecutors.newULimitCachedThreadPool(16)));

		ReadThrottleFilterBuilder throttleFilter = new ReadThrottleFilterBuilder();

		throttleFilter.setMaximumConnectionBufferSize(4 * 1024);
		throttleFilter.attach(filterChainBuilder);

		acceptor.setHandler(new GcsAcceptorProtocolHandler());

		// Bind
		acceptor.bind();

		String localAddr = acceptor.getLocalAddress().toString();
		log.info("{} listening on:{}", SERVICE_NAME, localAddr);
	}

	private void startConnector()
	{
		connector = new SocketConnector(IO_THREADS, CustomExecutors.newULimitCachedThreadPool(16));

		DefaultIoFilterChainBuilder filterChainBuilder = connector.getFilterChain();

		// Add CPU-bound job first,
		filterChainBuilder.addLast("GCS_CODEC", new ProtocolCodecFilter(new GcsCodec()));
		// and then a thread pool.
		filterChainBuilder.addLast("threadPool", new ExecutorFilter(CustomExecutors.newULimitCachedThreadPool(16)));

		connector.setHandler(new GcsRemoteProtocolHandler());
	}

	public void populateWorldMap()
	{
		_wmap = new WorldMap();

		List<Peer> peerList = _wmap.getPeerList();
		for (Peer peer : peerList)
		{
			// System.out.println("Gcs.init.peer.connect: " + peer.getName()
			// + ":" + peer.getHost() + ":" + peer.getPort());
			GcsExecutor.execute(new Connect(peer));
			// GcsRemoteConnector.connect(peer.getHost(), peer.getPort());
		}
		// Statistics.init();
		
	}

	public synchronized static void connect(String host, int port)
	{
		SocketAddress addr = new InetSocketAddress(host, port);
		connect(addr);
	}

	public static synchronized void connect(SocketAddress address)
	{
		String message = "Connecting to {}";
		log.info(message, address.toString());

		ConnectFuture cf = instance.connector.connect(address).awaitUninterruptibly();
		Sleep.time(2000);
		while (!cf.isConnected())
		{
			log.info(message, address.toString());
			cf = instance.connector.connect(address).awaitUninterruptibly();
			Sleep.time(2000);
		}
	}

	public static void publish(Message message)
	{
		message.setType(MessageType.COM_TOPIC);
		LocalTopicConsumers.notify(message);
		RemoteTopicConsumers.notify(message);
	}

	public static void enqueue(final Message message)
	{
		QueueProcessorList.get(message.getDestination()).process(message);
	}

	public static void addTopicConsumer(String topicName, MessageListener listener)
	{
		LocalTopicConsumers.add(topicName, listener);
	}

	public static void addQueueConsumer(String queueName, MessageListener listener)
	{
		if (StringUtils.contains(queueName, "@"))
		{
			String topicName = StringUtils.substringAfter(queueName, "@");
			MessageListener dispatcher = new TopicToQueueDispatcher(queueName);
			LocalTopicConsumers.add(topicName, dispatcher);
		}
		LocalQueueConsumers.add(queueName, listener);
	}

	public static void removeTopicConsumer(MessageListener listener)
	{
		LocalTopicConsumers.remove(listener);
	}

	public static void removeQueueConsumer(MessageListener listener)
	{
		LocalQueueConsumers.remove(listener);
	}

	public static List<Peer> getPeerList()
	{
		return Collections.unmodifiableList(instance._wmap.getPeerList());
	}

	public static Set<IoSession> getManagedConnectorSessions()
	{
		return Collections.unmodifiableSet(instance.connector.getManagedSessions());
	}

	public static Set<IoSession> getManagedAcceptorSessions()
	{
		return Collections.unmodifiableSet(instance.acceptor.getManagedSessions());
	}
}
