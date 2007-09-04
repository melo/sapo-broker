package pt.com.broker.core;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketSessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.Start;
import pt.com.broker.net.BrokerProtocolHandler;
import pt.com.gcs.Gcs;

public class BrokerServer
{
	private static Logger log = LoggerFactory.getLogger(BrokerServer.class);

	private int _portNumber;

	private ExecutorService ioExecutor;

	private ExecutorService eventExecutor;

	private static final int NCPU = Runtime.getRuntime().availableProcessors();

	private static final int DEFAULT_IO_THREADS = NCPU + 1;

	private static final int DEFAULT_EVENT_THREADS = 10;

	private int eventThreads = DEFAULT_EVENT_THREADS;

	public BrokerServer(int portNumber)
	{
		_portNumber = portNumber;
	}

	public void start()
	{
		try
		{
			log.info("Sapo-Broker STARTING");
			Gcs.init();

			ioExecutor = new ThreadPoolExecutor(DEFAULT_IO_THREADS, DEFAULT_IO_THREADS, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

			SocketAcceptor acceptor = new SocketAcceptor(NCPU, ioExecutor);

			acceptor.setReuseAddress(true);
			((SocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);

			acceptor.setBacklog(100);

			acceptor.setLocalAddress(new InetSocketAddress(_portNumber));

			eventExecutor = new ThreadPoolExecutor(eventThreads, eventThreads * NCPU, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
			acceptor.getFilterChain().addLast("threadPool", new ExecutorFilter(eventExecutor));

			acceptor.setHandler(new BrokerProtocolHandler());

			// Bind
			acceptor.bind();
			log.info("Listening on: " + acceptor.getLocalAddress());
		}
		catch (Throwable e)
		{
			log.error(e.getMessage(), e);
			Start.shutdown();
		}
	}

}
