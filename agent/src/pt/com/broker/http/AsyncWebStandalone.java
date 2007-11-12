package pt.com.broker.http;

import java.net.InetSocketAddress;

import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.safehaus.asyncweb.codec.HttpServerCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncWebStandalone
{
	private static final Logger LOG = LoggerFactory.getLogger(AsyncWebStandalone.class);

	private int _portNumber;

	private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() + 1;

	public AsyncWebStandalone(int portNumber)
	{
		_portNumber = portNumber;
	}

	public void start()
	{
		try
		{
			SocketAcceptor acceptor = new NioSocketAcceptor(DEFAULT_IO_THREADS);

			acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new HttpServerCodecFactory()));

			acceptor.setReuseAddress(true);
			((SocketSessionConfig) acceptor.getSessionConfig()).setReuseAddress(true);
			((SocketSessionConfig) acceptor.getSessionConfig()).setReceiveBufferSize(1024);
			((SocketSessionConfig) acceptor.getSessionConfig()).setSendBufferSize(1024);
			((SocketSessionConfig) acceptor.getSessionConfig()).setSoLinger(-1);
			acceptor.setBacklog(10240);

			acceptor.setLocalAddress(new InetSocketAddress(_portNumber));
			acceptor.setHandler(new HttpProtocolHandler());

			acceptor.bind();
			LOG.info("AsyncWeb server started.");
		}
		catch (Throwable e)
		{
			LOG.error("Failed to start HTTP container!", e);
			System.exit(1);
		}
	}
}
