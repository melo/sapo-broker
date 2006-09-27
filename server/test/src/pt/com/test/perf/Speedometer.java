package pt.com.test.perf;

import static java.lang.System.out;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import pt.com.broker.BrokerMessage;
import pt.com.broker.Notify;
import pt.com.ds.CacheFiller;
import pt.com.text.StringUtils;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public class Speedometer
{
	private static final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);

	static Socket client;

	static DataOutputStream rawo = null;

	static DataInputStream rawi = null;

	static int iter;

	static AtomicLong counter = new AtomicLong(0L);

	static AtomicLong prev_counter = new AtomicLong(0L);

	static final CacheFiller<String, TestCounter> ccfill = new CacheFiller<String, TestCounter>()
	{
		public TestCounter populate(String key)
		{
			return new TestCounter(1L);
		}
	};

	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");

		checkArgs(args);

		String ip = args[0];
		String port = args[1];
		String topic = args[2];
		String dtype = args[3];

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

		out.println("Waiting notifications");

		final Runnable speedcounter = new Runnable()
		{

			public void run()
			{
				long oldValue = prev_counter.get();
				long currentValue = counter.get();

				out.println(((float) Math.max(0, (currentValue - oldValue))) / 10 + " message(s)/sec");
				prev_counter.set(currentValue);

			}

		};

		exec.scheduleAtFixedRate(speedcounter, 0L, 10L, TimeUnit.SECONDS);

		while (true)
		{
			try
			{
				int len = rawi.readInt();
				byte[] b = new byte[len];
				rawi.readFully(b);
				BrokerMessage msg = SoapSerializer.FromXml(new ByteArrayInputStream(b)).body.notification.brokerMessage;
				counter.getAndIncrement();
			}
			catch (IOException ce)
			{
				reconnect(ip, port, topic, dtype, ce);
			}
		}
	}

	private static void checkArgs(String[] args) throws IllegalArgumentException
	{
		String errorMessage = "Missing arguments. Required arguments are IP, Port Number, Topic Name, Destination type";
		if (args.length < 3)
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

	private static void subscribe(String topic, String dtype) throws IOException
	{
		ByteArrayOutputStream bout = new ByteArrayOutputStream();

		SoapEnvelope soap = new SoapEnvelope();

		SoapHeader soap_header = new SoapHeader();
		EndPointReference epr = new EndPointReference();
		epr.address = "java://testconsumer/";
		soap_header.wsaFrom = epr;
		soap_header.wsaAction = "http://services.sapo.pt/broker/subscribe/";

		soap.header = soap_header;

		soap.body.notify = new Notify();
		soap.body.notify.destinationName = topic;
		soap.body.notify.destinationType = dtype;
		SoapSerializer.ToXml(soap, bout);

		byte[] message = bout.toByteArray();

		rawo.writeInt(message.length);
		rawo.write(message);
		rawo.flush();
	}

	private static void reconnect(String ip, String port, String topic, String dtype, Throwable se) throws InterruptedException
	{
		out.println("Connect Error");
		Throwable ex = new Exception(se);

		while (ex != null)
		{
			Thread.sleep(2000);
			try
			{
				out.println("Trying to reconnect");
				client = new Socket(ip, Integer.parseInt(port));
				rawo = new DataOutputStream(client.getOutputStream());
				rawi = new DataInputStream(client.getInputStream());
				subscribe(topic, dtype);
				ex = null;
				out.println("Waiting notification");
			}
			catch (Exception re)
			{
				out.println("Reconnect failled");
				ex = re;
			}
		}
	}
}
