package pt.com.manta;

import java.net.InetSocketAddress;

import org.apache.mina.transport.socket.nio.SocketAcceptor;
import org.mr.MantaAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MantaBusServer
{
	private static Logger log = LoggerFactory.getLogger(MantaBusServer.class);

	private int _portNumber;

	public MantaBusServer(int portNumber)
	{
		_portNumber = portNumber;
	}

	public void start(SocketAcceptor acceptor)
	{

		try
		{
			if (!MantaAgent.getInstance().init())
			{
				throw new RuntimeException("Mantaray could not start");
			}

			log.info("MANTARAY BUS STARTING");

			Thread.sleep(2500);

			Management mng = new Management();

			Dispatcher dispatcher = new Dispatcher();

			// Bind

			acceptor.bind(new InetSocketAddress(_portNumber), new MantaJmsProtocolHandler());
		}
		catch (Throwable e)
		{
			log.error(e.getMessage(), e);
			Start.shutdown();
		}

		log.info("Listening on port " + _portNumber);
	}

}
