package pt.com.broker.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.http.MinaHttpServer;

public class BrokerHttpService
{
	private static final Logger LOG = LoggerFactory.getLogger(BrokerHttpService.class);

	private int _portNumber;

	public BrokerHttpService(int portNumber)
	{
		_portNumber = portNumber;
	}

	public void start()
	{
		try
		{
			MinaHttpServer server = new MinaHttpServer();
			server.setPort(_portNumber);
			server.setRouter(new BrokerRequestRouter());
			server.start();
		}
		catch (Throwable e)
		{
			LOG.error("Failed to start HTTP container!", e);
			System.exit(1);
		}
	}
}
