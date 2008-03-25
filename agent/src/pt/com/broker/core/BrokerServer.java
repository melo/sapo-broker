package pt.com.broker.core;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.executor.IoEventQueueThrottle;
import org.apache.mina.filter.executor.OrderedThreadPoolExecutor;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.caudexorigo.Shutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.net.BrokerProtocolHandler;
import pt.com.broker.net.codec.SoapCodec;
import pt.com.gcs.messaging.Gcs;

public class BrokerServer
{
	private static Logger log = LoggerFactory.getLogger(BrokerServer.class);

	private int _portNumber;

	private static final int MAX_BUFFER_SIZE = 8 * 1024 * 1024;

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
			filterChainBuilder.addLast("SOAP_CODEC", new ProtocolCodecFilter(new SoapCodec()));
			filterChainBuilder.addLast("executor", new ExecutorFilter(new OrderedThreadPoolExecutor(0, 16, 30, TimeUnit.SECONDS, new IoEventQueueThrottle(MAX_BUFFER_SIZE))));

			acceptor.setHandler(new BrokerProtocolHandler());

			// Bind
			acceptor.bind(new InetSocketAddress(_portNumber));
			log.info("SAPO-BROKER Listening on: '{}'.", acceptor.getLocalAddress());
		}
		catch (Throwable e)
		{
			log.error(e.getMessage(), e);
			Shutdown.now();
		}
	}

}
