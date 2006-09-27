package pt.com.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import pt.com.broker.BrokerMessage;
import pt.com.broker.Notify;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public class Consumer
{

	static Socket client;

	static DataOutputStream rawo = null;

	static DataInputStream rawi = null;

	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");

		if (args.length < 3)
		{
			throw new IllegalArgumentException("Missing arguments: IP, Port Number or Topic");
		}

		String ip = args[0];
		String port = args[1];
		String topic = args[2];
		String dtype = args[3];

		if ((ip == null) || (port == null) || (topic == null) || (dtype == null))
		{
			throw new IllegalArgumentException("Missing arguments: IP, Port Number, Topic or Destination type");
		}

		if ((ip.trim().length() < 1) || (port.trim().length() < 1) || (topic.trim().length() < 1) || (dtype.trim().length() < 1))
		{
			throw new IllegalArgumentException("Missing arguments: IP, Port Number, Topic or Destination type");
		}

		try
		{

			client = new Socket(ip, Integer.parseInt(port));
			rawo = new DataOutputStream(client.getOutputStream());
			rawi = new DataInputStream(client.getInputStream());

			subscribe(topic, dtype);

		}
		catch (IOException ioe)
		{
			reconnect(ip, port, topic, dtype, ioe);
		}

		System.out.println("waiting notification");

		while (true)
		{
			try
			{
				int len = rawi.readInt();
				// System.out.print(len + "\n");
				byte[] b = new byte[len];
				rawi.readFully(b);
				//System.out.println(new String(b));
				BrokerMessage msg = SoapSerializer.FromXml(new ByteArrayInputStream(b)).body.notification.brokerMessage;
				System.out.println(msg.textPayload);
			}
			catch (IOException ce)
			{
				reconnect(ip, port, topic, dtype, ce);
			}
		}
	}

	private static void subscribe(String topic, String dtype) throws IOException
	{
		ByteArrayOutputStream bout = new ByteArrayOutputStream();

		SoapEnvelope soap = new SoapEnvelope();
		
		SoapHeader soap_header = new SoapHeader();
		EndPointReference epr = new EndPointReference();
		epr.address = "java://mysniffer/";
		soap_header.wsaFrom = epr;
		soap_header.wsaAction = "http://services.sapo.pt/broker/subscribe/";
		
		soap.header = soap_header;
		
		
		soap.body.notify = new Notify();
		soap.body.notify.destinationName = topic;
		soap.body.notify.destinationType = dtype;
		SoapSerializer.ToXml(soap, bout);

		byte[] message = bout.toByteArray();
		
		System.out.println(new String(message));

		rawo.writeInt(message.length);
		rawo.write(message);
		rawo.flush();
	}

	private static void reconnect(String ip, String port, String topic, String dtype, Throwable se) throws InterruptedException
	{
		System.out.println("Connect Error");
		Throwable ex = new Exception(se);

		while (ex != null)
		{
			Thread.sleep(2000);
			try
			{
				System.out.println("Trying to reconnect");
				client = new Socket(ip, Integer.parseInt(port));
				rawo = new DataOutputStream(client.getOutputStream());
				rawi = new DataInputStream(client.getInputStream());
				subscribe(topic, dtype);
				ex = null;
				System.out.println("waiting notification");
			}
			catch (Exception re)
			{
				System.out.println("Reconnect failled");
				ex = re;
			}
		}
	}
}
