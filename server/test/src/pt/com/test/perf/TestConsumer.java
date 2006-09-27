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

public class TestConsumer
{
	private static final Cache<String, TestCounter> counters = new Cache<String, TestCounter>();

	static AtomicLong parcial = new AtomicLong(0L);

	static AtomicLong total = new AtomicLong(0L);

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

		client = new Socket(ip, Integer.parseInt(port));
		rawo = new DataOutputStream(client.getOutputStream());
		rawi = new DataInputStream(client.getInputStream());

		subscribe(topic, dtype);

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
			int len = rawi.readInt();
			// System.out.print(len + "\n");
			byte[] b = new byte[len];
			rawi.readFully(b);
			BrokerMessage msg = SoapSerializer.FromXml(new ByteArrayInputStream(b)).body.notification.brokerMessage;
			String[] msg_parts = StringUtils.split(msg.textPayload, '$');
			String msgGroupName = msg_parts[0];
			TestCounter cn = counters.get(msgGroupName, ccfill);
			long cni = cn.counter.getAndIncrement();
			parcial.getAndIncrement();

			if (cni == iter)
			{
				long producerSendTimeStamp = Long.parseLong(msgGroupName);
				float duration = ((float) (Math.min((System.currentTimeMillis() - cn.start), producerSendTimeStamp))) / 1000;
				out.printf("Received all messages for %s in %6.3fs%n", msgGroupName, duration);
				counters.removeValue(cn);
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

	private static void subscribe(String dname, String dtype) throws IOException
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
		soap.body.notify.destinationName = dname;
		soap.body.notify.destinationType = dtype;
		SoapSerializer.ToXml(soap, bout);

		byte[] message = bout.toByteArray();

		rawo.writeInt(message.length);
		rawo.write(message);
		rawo.flush();
	}
}
