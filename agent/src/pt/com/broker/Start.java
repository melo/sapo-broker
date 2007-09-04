package pt.com.broker;

import org.apache.mina.common.ExceptionMonitor;

import pt.com.broker.core.BrokerServer;
import pt.com.broker.core.ErrorHandler;
import pt.com.broker.core.FilePublisher;
import pt.com.broker.http.AsyncWebStandalone;
import pt.com.gcs.conf.AgentInfo;

public class Start
{
	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");
		ExceptionMonitor.setInstance(new ErrorHandler());

		int bus_port = AgentInfo.getBrokerPort();
		BrokerServer broker_srv = new BrokerServer(bus_port);
		broker_srv.start();

		int http_port = AgentInfo.getBrokerHttpPort();
		AsyncWebStandalone http_srv = new AsyncWebStandalone(http_port);
		http_srv.start();

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
