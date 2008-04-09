package pt.com.gcs.messaging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.executor.IoEventQueueThrottle;
import org.apache.mina.filter.executor.OrderedThreadPoolExecutor;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.Shutdown;
import org.caudexorigo.concurrent.Sleep;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.conf.WorldMap;
import pt.com.gcs.net.Peer;
import pt.com.gcs.net.codec.GcsCodec;

public class Gcs
{
	private static Logger log = LoggerFactory.getLogger(Gcs.class);

	private static final int NCPU = Runtime.getRuntime().availableProcessors();

	private static final int IO_THREADS = NCPU + 1;

	private static final String SERVICE_NAME = "SAPO GCS";

	private static final int MAX_BUFFER_SIZE = 8 * 1024 * 1024;

	private static final Gcs instance = new Gcs();

	public static void ackMessage(String queueName, final String msgId)
	{
		instance.iackMessage(queueName, msgId);
	}

	public static void addAsyncConsumer(String destinationName, MessageListener listener)
	{
		if (listener.getDestinationType() == DestinationType.TOPIC)
		{
			instance.iaddTopicConsumer(destinationName, listener);
		}
		else if (listener.getDestinationType() == DestinationType.QUEUE)
		{
			instance.iaddQueueConsumer(destinationName, listener);
		}
	}

	protected static void connect(SocketAddress address)
	{
		String message = "Connecting to '{}'.";
		log.info(message, address.toString());
		
		ConnectFuture cf = instance.connector.connect(address).awaitUninterruptibly();

		if (!cf.isConnected())
		{
			GcsExecutor.schedule(new Connect(address), 5000, TimeUnit.MILLISECONDS);
		}
	}

	public static void enqueue(final Message message)
	{
		instance.ienqueue(message);
	}

	protected static Set<IoSession> getManagedAcceptorSessions()
	{
		return Collections.unmodifiableSet(instance.acceptor.getManagedSessions());
	}

	protected static Set<IoSession> getManagedConnectorSessions()
	{
		return Collections.unmodifiableSet(instance.connector.getManagedSessions());
	}

	protected static List<Peer> getPeerList()
	{
		return WorldMap.getPeerList();
	}
	
	public static void init()
	{
		instance.iinit();
	}

	public static Message poll(final String queueName)
	{	
		return instance.ipoll(queueName);
	}

	public static void publish(Message message)
	{
		instance.ipublish(message);
	}
	
	public static void releaseMessage(String queueName, String messageId)
	{
		QueueProcessorList.get(queueName).removeFromReservedMessages(messageId);
	}

	public static void removeAsyncConsumer(MessageListener listener)
	{
		if (listener.getDestinationType() == DestinationType.TOPIC)
		{
			LocalTopicConsumers.remove(listener);
		}
		else if (listener.getDestinationType() == DestinationType.QUEUE)
		{
			LocalQueueConsumers.remove(listener);
		}
	}

	public static void removeSyncConsumer(String queueName)
	{
		LocalQueueConsumers.removeSyncConsumer(queueName);
	}

	private SocketAcceptor acceptor;

	private SocketConnector connector;

	private Gcs()
	{
		log.info("{} starting.", SERVICE_NAME);
		try
		{

			startAcceptor(AgentInfo.getAgentPort());
			startConnector();

			GcsExecutor.scheduleWithFixedDelay(new QueueAwaker(), 5, 5, TimeUnit.SECONDS);
			GcsExecutor.scheduleWithFixedDelay(new QueueCounter(), 20, 20, TimeUnit.SECONDS);
			GcsExecutor.scheduleWithFixedDelay(new WorldMapMonitor(), 120, 120, TimeUnit.SECONDS);
		}
		catch (Throwable t)
		{
			Throwable rootCause = ErrorAnalyser.findRootCause(t);
			log.error(rootCause.getMessage(), rootCause);
			Shutdown.now();
		}
		Sleep.time(AgentInfo.getInitialDelay());

	}

	private void connectToAllPeers()
	{
		List<Peer> peerList = WorldMap.getPeerList();
		for (Peer peer : peerList)
		{
			SocketAddress addr = new InetSocketAddress(peer.getHost(), peer.getPort());
			connect(addr);
		}
	}
	
	private void iackMessage(String queueName, final String msgId)
	{
		QueueProcessorList.get(queueName).ack(msgId);
	}
	
	private void iaddQueueConsumer(String queueName, MessageListener listener)
	{
		QueueProcessorList.get(queueName);

		if (StringUtils.contains(queueName, "@"))
		{
			DispatcherList.create(queueName);
		}

		if (listener != null)
		{
			LocalQueueConsumers.add(queueName, listener);
		}
	}	

	private void iaddTopicConsumer(String topicName, MessageListener listener)
	{
		if (listener != null)
		{
			LocalTopicConsumers.add(topicName, listener, true);
		}
	}

	private void ienqueue(final Message message)
	{
		QueueProcessorList.get(message.getDestination()).store(message);
	}

	private void iinit()
	{
		String[] virtual_queues = DbStorage.getVirtualQueuesNames();

		for (String vqueue : virtual_queues)
		{
			log.debug("Add VirtualQueue '{}' from storage", vqueue);
			iaddQueueConsumer(vqueue, null);
		}
		
		connectToAllPeers();
		
		log.info("{} initialized.", SERVICE_NAME);
	}

	private Message ipoll(final String queueName)
	{
		LocalQueueConsumers.addSyncConsumer(queueName);
		return QueueProcessorList.get(queueName).poll();
	}

	private void ipublish(final Message message)
	{
		message.setType(MessageType.COM_TOPIC);
		LocalTopicConsumers.notify(message);
		RemoteTopicConsumers.notify(message);
	}
	
	private void startAcceptor(int portNumber) throws IOException
	{
		acceptor = new NioSocketAcceptor(IO_THREADS);

		acceptor.setReuseAddress(true);
		((SocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);
		((SocketSessionConfig) acceptor.getSessionConfig()).setTcpNoDelay(false);
		((SocketSessionConfig) acceptor.getSessionConfig()).setKeepAlive(true);

		acceptor.setBacklog(100);

		DefaultIoFilterChainBuilder filterChainBuilder = acceptor.getFilterChain();

		// Add CPU-bound job first,
		filterChainBuilder.addLast("GCS_CODEC", new ProtocolCodecFilter(new GcsCodec()));
		// and then a thread pool.
		filterChainBuilder.addLast("executor", new ExecutorFilter(new OrderedThreadPoolExecutor( 0, 16, 30, TimeUnit.SECONDS, new IoEventQueueThrottle())));

		acceptor.setHandler(new GcsAcceptorProtocolHandler());

		// Bind
		acceptor.bind(new InetSocketAddress(portNumber));

		String localAddr = acceptor.getLocalAddress().toString();
		log.info("{} listening on: '{}'.", SERVICE_NAME, localAddr);
	}
	
	private void startConnector()
	{
		connector = new NioSocketConnector(IO_THREADS);
		((SocketSessionConfig) connector.getSessionConfig()).setKeepAlive(true);

		DefaultIoFilterChainBuilder filterChainBuilder = connector.getFilterChain();

		// Add CPU-bound job first,
		filterChainBuilder.addLast("GCS_CODEC", new ProtocolCodecFilter(new GcsCodec()));

		// and then a thread pool.
		filterChainBuilder.addLast("executor", new ExecutorFilter(new OrderedThreadPoolExecutor(0, 16, 30, TimeUnit.SECONDS, new IoEventQueueThrottle(MAX_BUFFER_SIZE))));

		connector.setHandler(new GcsRemoteProtocolHandler());
		connector.setConnectTimeoutMillis(5000); // 5 seconds timeout
	}

}
