import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.caudexorigo.Shutdown;

import pt.com.broker.BrokerClient;
import pt.com.broker.messaging.BrokerListener;
import pt.com.broker.messaging.BrokerMessage;
import pt.com.broker.messaging.DestinationType;
import pt.com.broker.messaging.Notify;

public class MQConsumer implements BrokerListener
{

	public static void main(String[] args)
	{
		BrokerClient bk = new BrokerClient("localhost", 3322, "tcp://mycompany.com/mysniffer");
		
		MQConsumer consumer = new MQConsumer();
		
        Notify nreq1 = new Notify();
        nreq1.destinationName = "/test/a";
        nreq1.destinationType = DestinationType.TOPIC;
        
        try
		{
			bk.addAsyncConsumer(nreq1, consumer);
		}
		catch (Exception e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
			Shutdown.now();
		}
        
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        System.out.println("listening...");

        while (true)
        {			
            try
			{
				if (in.readLine().equalsIgnoreCase("exit"))
				{
					bk.close();
					Shutdown.now();
				}
			}
			catch (IOException e)
			{
				e.printStackTrace();
				Shutdown.now();
			}
        }

	}

	@Override
	public boolean isAutoAck()
	{
		return false;
	}

	@Override
	public void onMessage(BrokerMessage message)
	{
		System.out.println(message.textPayload);
	}

}
