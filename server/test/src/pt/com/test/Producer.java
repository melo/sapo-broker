package pt.com.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Random;

import pt.com.broker.Enqueue;
import pt.com.broker.Publish;
import pt.com.text.RandomStringUtils;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public class Producer
{
	private static final int DEFAULT_MAX_MESSAGE_SIZE = 128 * 1024;

	static Socket client;

	static final Random rnd = new Random();

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

		if ((ip == null) || (port == null) || (dname == null) || (dtype == null))
		{
			throw new IllegalArgumentException("Missing arguments: IP, Port Number, Topic or Destination type");
		}

		if ((ip.trim().length() < 1) || (port.trim().length() < 1) || (dname.trim().length() < 1) || (dtype.trim().length() < 1))
		{
			throw new IllegalArgumentException("Missing arguments: IP, Port Number, Topic or Destination type");
		}

		client = new Socket(ip, Integer.parseInt(port));

		DataOutputStream raw = new DataOutputStream(client.getOutputStream());

		System.out.println("Start sending");

		long start = System.currentTimeMillis();

		for (int i = 0; i < 100000; i++)
		{

			System.out.println("message:" + i);
			byte[] message = getMesssage(dname, dtype);
			raw.writeInt(message.length);
			raw.write(message);
			// raw.flush();
			Thread.sleep(1000);
		}

		long stop = System.currentTimeMillis();

		System.out.println("time:" + (stop - start));

		raw.close();
		client.close();
	}

	static byte[] getMesssage(String destinationName, String destinationType) throws UnsupportedEncodingException
	{

		SoapEnvelope soap = new SoapEnvelope();

		SoapHeader soap_header = new SoapHeader();
		EndPointReference epr = new EndPointReference();
		epr.address = "java://mydumper/";
		soap_header.wsaFrom = epr;
		soap_header.wsaAction = "http://services.sapo.pt/broker/publish/";

		int msize = rnd.nextInt( 250 - 200 + 1 ) + 200;

		soap.header = soap_header;

		if (destinationType.equals("TOPIC"))
		{
			soap.body.publish = new Publish();
			soap.body.publish.brokerMessage.destinationName = destinationName;
			soap.body.publish.brokerMessage.textPayload = RandomStringUtils.randomAlphabetic(msize);
		}
		else if (destinationType.equals("QUEUE"))
		{
			soap.body.enqueue = new Enqueue();
			soap.body.enqueue.brokerMessage.destinationName = destinationName;
			soap.body.enqueue.brokerMessage.textPayload = RandomStringUtils.randomAlphabetic(msize);
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
