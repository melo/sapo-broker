package pt.com.broker.client;

import org.caudexorigo.cli.Option;

public interface CliArgs
{
	@Option(shortName = "h", defaultValue = "localhost")
	String getHost();

	@Option(shortName = "p", defaultValue = "3322")
	int getPort();

	@Option(shortName = "n", defaultValue = "/test")
	String getDestination();

	@Option(shortName = "d", defaultValue = "TOPIC")
	String getDestinationType();

}