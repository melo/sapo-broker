package pt.com.gcs;

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
import org.apache.mina.filter.traffic.WriteThrottleFilter;
import org.apache.mina.filter.traffic.WriteThrottlePolicy;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.caudexorigo.concurrent.Sleep;
import org.caudexorigo.lang.ErrorAnalyser;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.conf.WorldMap;
import pt.com.gcs.io.DbStorage;
import pt.com.gcs.messaging.DispatcherList;
import pt.com.gcs.messaging.LocalQueueConsumers;
import pt.com.gcs.messaging.LocalTopicConsumers;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.messaging.MessageListener;
import pt.com.gcs.messaging.MessageType;
import pt.com.gcs.messaging.QueueProcessor;
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

	private static final int ONE_K = 1024;

	private static final Gcs instance = new Gcs();

	private SocketAcceptor acceptor;

	private SocketConnector connector;

	private WorldMap _wmap;

	private Gcs()
	{
		log.info("{} starting.", SERVICE_NAME);
		try
		{
			DbStorage.init();
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
		acceptor = new NioSocketAcceptor(IO_THREADS);

		acceptor.setReuseAddress(true);
		((SocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);
		((SocketSessionConfig) acceptor.getSessionConfig()).setTcpNoDelay(false);

		acceptor.setBacklog(100);

		DefaultIoFilterChainBuilder filterChainBuilder = acceptor.getFilterChain();

		WriteThrottleFilter writeThrottleFilter = new WriteThrottleFilter(WriteThrottlePolicy.BLOCK, 0, 128 * ONE_K, 0, 256 * ONE_K, 0, 512 * ONE_K);

		// Add CPU-bound job first,
		filterChainBuilder.addLast("GCS_CODEC", new ProtocolCodecFilter(new GcsCodec()));
		// and then a thread pool.
		// filterChainBuilder.addLast("executor", new
		// ExecutorFilter(CustomExecutors.newThreadPool(16)));

		filterChainBuilder.addLast("executor", new ExecutorFilter(new OrderedThreadPoolExecutor(0, 16, 30, TimeUnit.SECONDS,
				new IoEventQueueThrottle(4 * 65536))));

		filterChainBuilder.addLast("writeThrottleFilter", writeThrottleFilter);

		acceptor.setHandler(new GcsAcceptorProtocolHandler());

		// Bind
		acceptor.bind(new InetSocketAddress(portNumber));

		String localAddr = acceptor.getLocalAddress().toString();
		log.info("{} listening on:{}.", SERVICE_NAME, localAddr);
	}

	private void startConnector()
	{
		connector = new NioSocketConnector(IO_THREADS);

		DefaultIoFilterChainBuilder filterChainBuilder = connector.getFilterChain();

		// Add CPU-bound job first,
		filterChainBuilder.addLast("GCS_CODEC", new ProtocolCodecFilter(new GcsCodec()));

		// and then a thread pool.
		// ReadThrottleFilter readThrottleFilter = new
		// ReadThrottleFilter(Executors.newSingleThreadScheduledExecutor(),
		// ReadThrottlePolicy.BLOCK, 128 * ONE_K, 256 * ONE_K, 512 * ONE_K);
		// filterChainBuilder.addLast("executor", new
		// ExecutorFilter(CustomExecutors.newThreadPool(16)));
		// filterChainBuilder.addLast("readThrottleFilter", readThrottleFilter);

		filterChainBuilder.addLast("executor", new ExecutorFilter(new OrderedThreadPoolExecutor(0, 16, 30, TimeUnit.SECONDS,
				new IoEventQueueThrottle(2 * 65536))));

		connector.setHandler(new GcsRemoteProtocolHandler());
		// connector.setConnectTimeout(2);
	}

	public void populateWorldMap()
	{
		_wmap = new WorldMap();

		List<Peer> peerList = _wmap.getPeerList();
		for (Peer peer : peerList)
		{
			GcsExecutor.execute(new Connect(peer));
		}
		// Statistics.init();
	}

	public static void connect(String host, int port)
	{
		SocketAddress addr = new InetSocketAddress(host, port);
		connect(addr);
	}

	public static void connect(SocketAddress address)
	{
		String message = "Connecting to '{}'.";
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

	public static void init()
	{
		instance.iinit();
	}

	private void iinit()
	{
		log.info("{} initialized.", SERVICE_NAME);
	}

	public static void publish(Message message)
	{
		instance.ipublish(message);
	}

	private void ipublish(Message message)
	{
		message.setType(MessageType.COM_TOPIC);
		LocalTopicConsumers.notify(message);
		RemoteTopicConsumers.notify(message);
	}

	public static void enqueue(final Message message)
	{
		instance.ienqueue(message);
	}

	private void ienqueue(final Message message)
	{
		QueueProcessorList.get(message.getDestination()).process(message);
	}

	public static void ackMessage(final String msgId)
	{
		instance.iackMessage(msgId);
	}

	private void iackMessage(final String msgId)
	{
		QueueProcessor.ack(msgId);
	}

	public static void addTopicConsumer(String topicName, MessageListener listener)
	{
		instance.iaddTopicConsumer(topicName, listener);
	}

	private void iaddTopicConsumer(String topicName, MessageListener listener)
	{
		LocalTopicConsumers.add(topicName, listener);
	}

	public static void addQueueConsumer(String queueName, MessageListener listener)
	{
		instance.iaddQueueConsumer(queueName, listener);
	}

	private void iaddQueueConsumer(String queueName, MessageListener listener)
	{
		if (StringUtils.contains(queueName, "@"))
		{
			DispatcherList.add(queueName);
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
