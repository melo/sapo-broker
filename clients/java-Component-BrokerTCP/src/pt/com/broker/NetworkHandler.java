package pt.com.broker;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.DefaultIoFilterChainBuilder;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.messaging.BrokerMessage;
import pt.com.broker.messaging.Status;
import pt.com.broker.net.codec.SoapCodec;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapFault;

public class NetworkHandler
{
	private static final Logger log = LoggerFactory.getLogger(NetworkHandler.class);

	private static final int NCPU = Runtime.getRuntime().availableProcessors();

	private static final int MAX_BUFFER_SIZE = 32 * 1024 * 1024;

	private static final int IO_THREADS = NCPU + 1;

	private final SocketConnector connector;
	private final String _host;
	private final int _portNumber;
	private IoSession _ioSession;
	private final BrokerClient _brokerClient;

	private final AtomicBoolean _waitResponse = new AtomicBoolean(false);

	public NetworkHandler(BrokerClient brokerClient)
	{
		_host = brokerClient.getHost();
		_portNumber = brokerClient.getPort();

		connector = new NioSocketConnector(IO_THREADS);
		((SocketSessionConfig) connector.getSessionConfig()).setKeepAlive(true);

		DefaultIoFilterChainBuilder filterChainBuilder = connector.getFilterChain();

		// Add CPU-bound job first,
		filterChainBuilder.addLast("GCS_CODEC", new ProtocolCodecFilter(new SoapCodec()));

		// and then a thread pool.
		filterChainBuilder.addLast("executor", new ExecutorFilter(16, 30));

		connector.setHandler(new BrokerProtocolHandler(this));
		connector.setConnectTimeoutMillis(5000); // 5 seconds timeout

		SocketAddress addr = new InetSocketAddress(_host, _portNumber);
		connect(connector, addr);

		_brokerClient = brokerClient;
	}

	protected void connect(SocketConnector connector, SocketAddress address)
	{
		String message = "Connecting to '{}'.";
		log.info(message, address.toString());

		ConnectFuture cf = connector.connect(address);

		cf.awaitUninterruptibly();

		if (!cf.isConnected())
		{
			log.warn("Could not connect to '{}'", address.toString());
			BrokerClientExecutor.schedule(new Connect(this, address), 5000, TimeUnit.MILLISECONDS);
			return;

		}

		if (cf.isConnected())
		{
			if (connector.getManagedSessionCount() == 1)
			{
				for (IoSession ios : connector.getManagedSessions())
				{
					_ioSession = ios;
				}
			}

			if (_brokerClient != null)
			{
				_brokerClient.bindConsumers();
			}
		}
	}

	protected void sendMessage(SoapEnvelope soap)
	{
		sendMessage(soap, false, false);
	}

	protected void sendMessage(SoapEnvelope soap, boolean sendAsync)
	{
		sendMessage(soap, false, sendAsync);
	}

	protected void sendMessage(SoapEnvelope soap, boolean waitResponse, boolean sendAsync)
	{
		if (_ioSession != null && _ioSession.isConnected())
		{
			if (sendAsync)
			{
				_ioSession.write(soap);
			}
			else
			{
				WriteFuture wf = _ioSession.write(soap);
				wf.awaitUninterruptibly(5000, TimeUnit.MILLISECONDS);
				if (!wf.isWritten())
				{
					throw new RuntimeException("Message could not be written");
				}
			}
			_waitResponse.set(waitResponse);
		}
		else
		{
			throw new IllegalStateException("The Connection to the Broker is closed");
		}
	}

	protected void handleReceivedMessage(IoSession ioSession, SoapEnvelope request) throws Throwable
	{
		if (request.body.notification != null)
		{
			BrokerMessage msg = request.body.notification.brokerMessage;

			if (msg != null)
			{
				if (_waitResponse.getAndSet(false))
				{
					_brokerClient.feedSyncConsumer(msg);
				}
				else
				{
					_brokerClient.notifyListener(msg);
				}
			}
		}
		else if (request.body.status != null)
		{
			Status status = request.body.status;
			_brokerClient.feedStatusConsumer(status);
		}
		else if (request.body.fault != null)
		{
			SoapFault fault = request.body.fault;
			log.error(fault.toString());
			throw new RuntimeException(fault.faultReason.text);
		}

	}

	public SocketConnector getConnector()
	{
		return connector;
	}

	public void close()
	{
		try
		{
			for (IoSession ios : connector.getManagedSessions())
			{
				try
				{
					ios.close().await();
				}
				catch (InterruptedException e)
				{
					log.error(e.getMessage());
				}
			}
			connector.dispose();
		}
		catch (Throwable e)
		{
			// ignore
		}

	}

}
