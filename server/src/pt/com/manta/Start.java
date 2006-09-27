package pt.com.manta;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.ExceptionMonitor;
import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.apache.mina.transport.socket.nio.SocketAcceptorConfig;

import pt.com.broker.Statistics;
import pt.com.http.AsyncWebStandalone;
import pt.com.io.UnsynchByteBufferAllocator;

public class Start
{
	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");
		ByteBuffer.setAllocator(new UnsynchByteBufferAllocator());
		ExceptionMonitor.setInstance(new ErrorHandler());

		int ioWorkerCount = Runtime.getRuntime().availableProcessors();

		SocketAcceptor acceptor = new SocketAcceptor(ioWorkerCount);
		SocketAcceptorConfig sconfig = ((SocketAcceptorConfig) acceptor.getDefaultConfig());
		sconfig.setReuseAddress(true);

		int bus_port = Env.portFromSys("bus_port");
		MantaBusServer broker_srv = new MantaBusServer(bus_port);
		broker_srv.start(acceptor);

		int http_port = Env.portFromSys("http_port");
		AsyncWebStandalone http_srv = new AsyncWebStandalone(http_port);
		http_srv.start(acceptor);

		// WorldMapReloader wr = WorldMapReloader.getInstance();
		// wr.checkFileModifications();
		Statistics.init();
		FilePublisher.init();

	}

	public static void shutdown()
	{
		while (true)
		{
			System.exit(-1);
		}
	}
}
