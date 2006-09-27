package pt.com.test.perf;

import static java.lang.System.out;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import pt.com.ds.Cache;
import pt.com.ds.CacheFiller;
import pt.com.manta.MQLink;
import pt.com.text.StringUtils;

public class NativeConsumer implements MessageListener
{
	static Connection _connection;

	static Session _auto_ack_session;

	static Queue _queue;

	private static MessageConsumer _consumer;

	static int _iter;

	static AtomicLong counter = new AtomicLong(0L);

	static AtomicLong prev_counter = new AtomicLong(0L);

	private static final Cache<String, TestCounter> counters = new Cache<String, TestCounter>();

	private static final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);

	static final CacheFiller<String, TestCounter> ccfill = new CacheFiller<String, TestCounter>()
	{
		public TestCounter populate(String key)
		{
			return new TestCounter(1L);
		}
	};

	static final Runnable speedcounter = new Runnable()
	{

		public void run()
		{
			long oldValue = prev_counter.get();
			long currentValue = counter.get();

			out.println(Math.max(0, (currentValue - oldValue) / 10) + " message(s)/sec");
			prev_counter.set(currentValue);
		}

	};

	private static void checkArgs(String[] args) throws IllegalArgumentException
	{
		String errorMessage = "Missing arguments. Destination Name and #Iterations.";
		if (args.length < 2)
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

	public static void main(String[] args) throws Throwable
	{
		checkArgs(args);

		String queueName = args[0];
		_iter = Integer.parseInt(args[1]);

		NativeConsumer mantaConsumer = new NativeConsumer();
		
		try
		{
			_connection = MQLink.getJMSConnection();

			_auto_ack_session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			_queue = _auto_ack_session.createQueue(queueName);
			_consumer = _auto_ack_session.createConsumer(_queue);
			_consumer.setMessageListener(mantaConsumer);
		}
		catch (JMSException e)
		{
			e.printStackTrace();
			System.exit(-1);
		}

		Thread.sleep(2000);

		exec.scheduleAtFixedRate(speedcounter, 0L, 10L, TimeUnit.SECONDS);
	}

	public NativeConsumer()
	{

	}

	protected TextMessage buildMessage(Message amsg)
	{
		if (amsg instanceof TextMessage)
		{
			return (TextMessage) amsg;
		}
		else
		{
			return null;
		}
	}

	public void onMessage(Message amsg)
	{
		try
		{
			TextMessage msg = buildMessage(amsg);

			if (msg != null)
			{
				String[] msg_parts = StringUtils.split(msg.getText(), '$');
				String msgGroupName = msg_parts[0];
				TestCounter cn = counters.get(msgGroupName, ccfill);
				long cni = cn.counter.getAndIncrement();
				counter.getAndIncrement();

				if (cni == _iter)
				{
					long producerSendTimeStamp = Long.parseLong(msgGroupName);
					float duration = ((float) (Math.min((System.currentTimeMillis() - cn.start), producerSendTimeStamp))) / 1000;
					out.printf("Received all messages for %s in %6.3fs%n", msgGroupName, duration);
					counters.removeValue(cn);
				}
			}
			else
			{
				out.println("message is null");
			}
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

}
