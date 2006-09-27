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
import pt.com.ds.Cache;
import pt.com.ds.CacheFiller;
import pt.com.text.StringUtils;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public class TestSlowConsumer
{
	private static final Cache<String, TestCounter> counters = new Cache<String, TestCounter>();

	static Socket client;

	static DataOutputStream rawo = null;

	static DataInputStream rawi = null;

	static int iter;

	static final CacheFiller<String, TestCounter> ccfill = new CacheFiller<String, TestCounter>()
	{
		public TestCounter populate(String key)
		{
			return new TestCounter(1L);
		}
	};

	static AtomicLong parcial = new AtomicLong(0L);
	static AtomicLong total = new AtomicLong(0L);
	
	private static final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);

	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");

		checkArgs(args);

		String ip = args[0];
		String port = args[1];
		String topic = args[2];
		String dtype = args[3];
		iter = Integer.parseInt(args[4]);

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

		out.println("Waiting notification");

		final Runnable speedcounter = new Runnable()
		{
			public void run()
			{
				total.getAndAdd(parcial.get());
				out.print((parcial.getAndSet(0) / 10) + " messages/sec. ");
				out.println("Total: "+ (total.get() / 10) + " messages. ");
			}
		};

		exec.scheduleAtFixedRate(speedcounter, 0L, 10L, TimeUnit.SECONDS);

		while (true)
		{
			try
			{
				int len = rawi.readInt();
				// System.out.print(len + "\n");
				byte[] b = new byte[len];
				rawi.readFully(b);
				BrokerMessage msg = SoapSerializer.FromXml(new ByteArrayInputStream(b)).body.notification.brokerMessage;
				
				parcial.getAndIncrement();
				Thread.sleep(500);
			}
			catch (Exception ce)
			{
				reconnect(ip, port, topic, dtype, ce);
			}
		}
	}

	private static void checkArgs(String[] args) throws IllegalArgumentException
	{
		String errorMessage = "Missing arguments. Required arguments are IP, Port Number, Topic Name, Destination type and #Iterations";
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
