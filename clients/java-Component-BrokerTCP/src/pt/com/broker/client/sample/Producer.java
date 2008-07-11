package pt.com.broker.client.sample;

import java.util.concurrent.atomic.AtomicInteger;

import org.caudexorigo.cli.CliFactory;
import org.caudexorigo.concurrent.Sleep;
import org.caudexorigo.text.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.client.BrokerClient;
import pt.com.broker.client.messaging.BrokerMessage;
import pt.com.broker.client.messaging.DestinationType;

public class Producer
{
	private static final Logger log = LoggerFactory.getLogger(Producer.class);
	private final AtomicInteger counter = new AtomicInteger(0);

	private String host;
	private int port;
	private DestinationType dtype;
	private String dname;

	public static void main(String[] args) throws Throwable
	{
		final CliArgs cargs = CliFactory.parseArguments(CliArgs.class, args);
		Producer producer = new Producer();

		producer.host = cargs.getHost();
		producer.port = cargs.getPort();
		producer.dtype = DestinationType.valueOf(cargs.getDestinationType());
		producer.dname = cargs.getDestination();

		BrokerClient bk = new BrokerClient(producer.host, producer.port, "tcp://mycompany.com/mypublisher");
		
		log.info("Start sending a string of 100 random alphanumeric characters at the rate of 1/sec.");

		producer.sendLoop(bk);

	}

	private void sendLoop(BrokerClient bk) throws Throwable
	{
		for (int i = 0; i < 1000; i++)
		{
			final String msg = RandomStringUtils.randomAlphanumeric(100);
			BrokerMessage brkMsg = new BrokerMessage();

			brkMsg.textPayload = msg;
			brkMsg.destinationName = dname;
			if (dtype == DestinationType.QUEUE)
			{
				bk.enqueueMessage(brkMsg);
			}
			else
			{
				bk.publishMessage(brkMsg);
			}

			log.info(String.format("%s -> Send Message: %s", counter.incrementAndGet(), msg));
			Sleep.time(1000);
		}
	}
}