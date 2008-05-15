import org.caudexorigo.Shutdown;

import pt.com.broker.BrokerClient;
import pt.com.broker.messaging.BrokerMessage;
import pt.com.broker.messaging.Poll;

public class MQSyncConsumer
{
	public static void main(String[] args)
	{
		BrokerClient bk = new BrokerClient("localhost", 3322, "tcp://mycompany.com/mysniffer");

		Poll p = new Poll();
		p.destinationName = "/perf/test";

		while (true)
		{
			try
			{
				BrokerMessage m = bk.poll(p);
				System.out.println(m.textPayload);
				bk.acknowledge(m);
			}
			catch (Exception e)
			{
				Shutdown.now();
			}
		}

	}

}
