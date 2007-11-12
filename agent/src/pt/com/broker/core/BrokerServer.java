package pt.com.broker.core;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.executor.IoEventQueueThrottle;
import org.apache.mina.filter.executor.OrderedThreadPoolExecutor;
import org.apache.mina.filter.traffic.ReadThrottleFilter;
import org.apache.mina.filter.traffic.ReadThrottlePolicy;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.caudexorigo.concurrent.CustomExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.Start;
import pt.com.broker.net.BrokerProtocolHandler;
import pt.com.broker.net.codec.SoapCodec;
import pt.com.gcs.Gcs;
import pt.com.gcs.io.DbStorage;

public class BrokerServer
{
	private static Logger log = LoggerFactory.getLogger(BrokerServer.class);

	private int _portNumber;

	private static final int NCPU = Runtime.getRuntime().availableProcessors();

	public BrokerServer(int portNumber)
	{
		_portNumber = portNumber;
	}

	public void start()
	{
		try
		{
			log.info("Sapo-Broker starting.");
			DbStorage.init();
			Gcs.init();

			SocketAcceptor acceptor = new NioSocketAcceptor(NCPU);

			acceptor.setReuseAddress(true);
			((SocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);
			//acceptor.setBacklog(100);
			acceptor.setLocalAddress(new InetSocketAddress(_portNumber));

			DefaultIoFilterChainBuilder filterChainBuilder = acceptor.getFilterChain();
			// ReadThrottleFilter readThrottleFilter = new ReadThrottleFilter(ReadThrottlePolicy.BLOCK, 16 * 2048, 16 * 4096, 16 * 8192);
			// WriteThrottleFilter writeThrottleFilter = new
			// WriteThrottleFilter(WriteThrottlePolicy.BLOCK, 0, 16 * 2048, 0,
			// 16 * 4096, 0, 16 * 8192);

			// filterChainBuilder.addLast("writeThrottleFilter",
			// writeThrottleFilter);
			filterChainBuilder.addLast("SOAP_CODEC", new ProtocolCodecFilter(new SoapCodec()));
			//filterChainBuilder.addLast("executer", new ExecutorFilter(CustomExecutors.newThreadPool(16)));

			filterChainBuilder.addLast("executor", new ExecutorFilter(new OrderedThreadPoolExecutor(0, 16, 30, TimeUnit.SECONDS, new IoEventQueueThrottle())));

			// filterChainBuilder.addLast("readThrottleFilter", readThrottleFilter);
			acceptor.setHandler(new BrokerProtocolHandler());

			// Bind
			acceptor.bind();
			log.info("Listening on: '{}'.", acceptor.getLocalAddress());
		}
		catch (Throwable e)
		{
			log.error(e.getMessage(), e);
			Start.shutdown();
		}
	}

}
