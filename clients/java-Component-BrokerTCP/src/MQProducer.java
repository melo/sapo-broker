import org.caudexorigo.concurrent.Sleep;
import org.caudexorigo.text.RandomStringUtils;

import pt.com.broker.BrokerClient;
import pt.com.broker.messaging.BrokerMessage;


public class MQProducer
{

	public static void main(String[] args)
	{
		BrokerClient bk = new BrokerClient("localhost", 3322, "tcp://mycompany.com/mypublisher");
		
		for (int i = 0; i < 1000; i++)
		{
			final String msg =  RandomStringUtils.randomAlphabetic(100);
            BrokerMessage brkMsg = new BrokerMessage();

            brkMsg.textPayload = msg;
            brkMsg.destinationName = "/sapo/services/bricolage";
            try
			{
				bk.publishMessage(brkMsg);
			}
			catch (Exception e)
			{

				e.printStackTrace();
			}
            System.out.println(msg);
            Sleep.time(500);
		}

	}
}