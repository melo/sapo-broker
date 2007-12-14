package pt.com.broker.core;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.traffic.ReadThrottleFilter;
import org.apache.mina.filter.traffic.ReadThrottlePolicy;
import org.apache.mina.filter.traffic.WriteThrottleFilter;
import org.apache.mina.filter.traffic.WriteThrottlePolicy;
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

public class BrokerServer
{
	private static Logger log = LoggerFactory.getLogger(BrokerServer.class);

	private int _portNumber;
	
	private static final int ONE_K = 1024;

	private static final int NCPU = Runtime.getRuntime().availableProcessors();

	public BrokerServer(int portNumber)
	{
		_portNumber = portNumber;
	}

	public void start()
	{
		try
		{
			log.info("SAPO-BROKER starting.");
			
			Gcs.init();
			SocketAcceptor acceptor = new NioSocketAcceptor(NCPU);

			acceptor.setReuseAddress(true);
			((SocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);

			DefaultIoFilterChainBuilder filterChainBuilder = acceptor.getFilterChain();
			ReadThrottleFilter readThrottleFilter = new ReadThrottleFilter(Executors.newSingleThreadScheduledExecutor(), ReadThrottlePolicy.BLOCK, 256 * ONE_K, 512 * ONE_K, 1024 * ONE_K);
			WriteThrottleFilter writeThrottleFilter = new WriteThrottleFilter(WriteThrottlePolicy.BLOCK, 0, 1024 * ONE_K, 0, 2048 * ONE_K, 0, 4096 * ONE_K);

			filterChainBuilder.addLast("writeThrottleFilter", writeThrottleFilter);
			filterChainBuilder.addLast("SOAP_CODEC", new ProtocolCodecFilter(new SoapCodec()));
			filterChainBuilder.addLast("executor", new ExecutorFilter(CustomExecutors.newThreadPool(16)));
			filterChainBuilder.addLast("readThrottleFilter", readThrottleFilter);
			acceptor.setHandler(new BrokerProtocolHandler());

			// Bind
			acceptor.bind(new InetSocketAddress(_portNumber));
			log.info("SAPO-BROKER Listening on: '{}'.", acceptor.getLocalAddress());
		}
		catch (Throwable e)
		{
			log.error(e.getMessage(), e);
			Start.shutdown();
		}
	}

}
