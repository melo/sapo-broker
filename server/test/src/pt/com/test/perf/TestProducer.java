package pt.com.test.perf;

import static java.lang.System.out;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import pt.com.broker.BrokerMessage;
import pt.com.broker.Enqueue;
import pt.com.broker.Publish;
import pt.com.text.RandomStringUtils;
import pt.com.text.StringUtils;
import pt.com.xml.EndPointReference;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapHeader;
import pt.com.xml.SoapSerializer;

public class TestProducer implements Runnable
{
	private static final ExecutorService exec = Executors.newFixedThreadPool(10);

	private static final ScheduledExecutorService shed_exec = Executors.newScheduledThreadPool(1);

	static final Random rnd = new Random();

	static String ip;

	static String port;

	static String dname;

	static String dtype;

	static int iter;

	int mi;
	
	static int topic_suffix = 0;

	static AtomicLong parcial = new AtomicLong(0L);
	static AtomicLong total = new AtomicLong(0L);

	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");

		checkArgs(args);

		ip = args[0];
		port = args[1];
		dname = args[2];
		dtype = args[3];
		iter = Integer.parseInt(args[4]);

		final Runnable speedcounter = new Runnable()
		{
			public void run()
			{
				total.getAndAdd(parcial.get());
				out.print((parcial.getAndSet(0) / 10) + " messages/sec. ");
				out.println("Total: "+ (total.get() / 10) + " messages. ");
			}
		};
		shed_exec.scheduleAtFixedRate(speedcounter, 0L, 10L, TimeUnit.SECONDS);

		while (true)
		{
			TestProducer task = new TestProducer();
			exec.execute(task);
			// task.run();
			Thread.sleep(5000);
		}
	}

	private static void checkArgs(String[] args) throws IllegalArgumentException
	{
		String errorMessage = "Missing arguments. Required arguments are IP, Port Number, Destination Name, Destination type and #Iterations";
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

	public void run()
	{
		DataOutputStream raw = null;
		Socket client = null;
		try
		{
			client = new Socket(ip, Integer.parseInt(port));

			raw = new DataOutputStream(client.getOutputStream());

			long gid = System.currentTimeMillis();

			out.print("Start sending MessageGroup-" + gid);
			out.println(" ");
			for (mi = 0; mi < iter; mi++)
			{
				byte[] message = getMesssage(dname, dtype, gid);
				raw.writeInt(message.length);
				raw.write(message);
				parcial.getAndIncrement();
			}
			out.println("End sending MessageGroup-" + gid);
		}
		catch (Throwable e)
		{
			e.printStackTrace();
			try
			{
				raw.close();
				client.close();
			}
			catch (IOException ioe)
			{
				ioe.printStackTrace();
			}
		}
	}

	byte[] getMesssage(String destinationName, String destinationType, long mgid) throws UnsupportedEncodingException
	{

		SoapEnvelope soap = new SoapEnvelope();
		BrokerMessage bkmsg = new BrokerMessage();
		bkmsg.destinationName = destinationName;
		int msize = rnd.nextInt(250 - 200 + 1) + 200;
		final String lixo = RandomStringUtils.randomAlphabetic(msize);
		bkmsg.textPayload = mgid + "$MessageId-" + mi + "$Content: " + lixo;

		SoapHeader soap_header = new SoapHeader();
		EndPointReference epr = new EndPointReference();
		epr.address = "java://testproducer/";

		soap.header = soap_header;

		if (destinationType.equals("TOPIC"))
		{
			soap.body.publish = new Publish();
			soap.body.publish.brokerMessage = bkmsg;
			soap_header.wsaAction = "http://services.sapo.pt/broker/publish/";
		}
		else if (destinationType.equals("QUEUE"))
		{
			soap.body.enqueue = new Enqueue();
			soap.body.enqueue.brokerMessage = bkmsg;
			soap_header.wsaAction = "http://services.sapo.pt/broker/enqueue/";
		}
		else
		{
			throw new IllegalArgumentException("Not a valid destination type!");
		}

		soap_header.wsaFrom = epr;
		ByteArrayOutputStream bout = new ByteArrayOutputStream(2500);
		SoapSerializer.ToXml(soap, bout);

		return bout.toByteArray();
	}
}
