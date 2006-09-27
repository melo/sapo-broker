package pt.com.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Random;

import pt.com.broker.BrokerMessage;
import pt.com.broker.Enqueue;
import pt.com.broker.Publish;
import pt.com.text.RandomStringUtils;
import pt.com.text.StringUtils;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapSerializer;

public class FastProducer
{
	static final Random rnd = new Random();

	static Socket client;

	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");

		if (args.length < 3)
		{
			throw new IllegalArgumentException("Missing arguments: IP, Port Number or Topic");
		}

		String ip = args[0];
		String port = args[1];
		String dname = args[2];
		String dtype = args[3];

		checkArgs(args);

		client = new Socket(ip, Integer.parseInt(port));

		DataOutputStream raw = new DataOutputStream(client.getOutputStream());

		System.out.println("Start sending");

		long i = 0;
		while (true)
		{
			System.out.println("message:" + ++i);
			byte[] message = getMesssage(dname, dtype);
			raw.writeInt(message.length);
			raw.write(message);
			Thread.sleep(10);
		}

		// raw.close();
		// client.close();
	}

	private static void checkArgs(String[] args) throws IllegalArgumentException
	{
		String errorMessage = "Missing arguments. Required arguments are IP, Port Number, Destination Name, Destination type";
		if (args.length < 4)
		{
			throw new IllegalArgumentException(errorMessage);
		}
		for (int i = 0; i < args.length; i++)
		{
			if (StringUtils.isBlank(args[i]))
			{
				throw new IllegalArgumentException(errorMessage);
			}
		}
	}

	static byte[] getMesssage(String destinationName, String destinationType) throws UnsupportedEncodingException
	{

		SoapEnvelope soap = new SoapEnvelope();
		BrokerMessage bkmsg = new BrokerMessage();
		bkmsg.destinationName = destinationName;

		int msize = rnd.nextInt(250 - 200 + 1) + 200;
		bkmsg.textPayload = RandomStringUtils.randomAlphabetic(msize);

		if (destinationType.equals("TOPIC"))
		{
			soap.body.publish = new Publish();
			soap.body.publish.brokerMessage = bkmsg;
		}
		else if (destinationType.equals("QUEUE"))
		{
			soap.body.enqueue = new Enqueue();
			soap.body.publish.brokerMessage = bkmsg;
		}
		else
		{
			throw new IllegalArgumentException("Not a valid destination type!");
		}

		ByteArrayOutputStream bout = new ByteArrayOutputStream(2500);
		SoapSerializer.ToXml(soap, bout);

		return bout.toByteArray();
	}
}
