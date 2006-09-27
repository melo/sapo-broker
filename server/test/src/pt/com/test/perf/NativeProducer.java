package pt.com.test.perf;

import static java.lang.System.out;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import pt.com.manta.MQLink;
import pt.com.text.RandomStringUtils;
import pt.com.text.StringUtils;

public class NativeProducer
{
	private static ExecutorService exec = Executors.newFixedThreadPool(20);

	private static Connection _connection;

	private static Session _auto_ack_session;

	private static Queue _queue;

	private static MessageProducer _producer;

	static int _iter;

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

		try
		{
			_connection = MQLink.getJMSConnection();
			_auto_ack_session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			_queue = _auto_ack_session.createQueue(queueName);
			_producer = _auto_ack_session.createProducer(_queue);
		}
		catch (JMSException e)
		{
			e.printStackTrace();
			System.exit(-1);
		}

		final Runnable messageSender = new Runnable()
		{
			public void run()
			{
				try
				{
					long gid = System.currentTimeMillis();
					out.print("Start sending MessageGroup-" + gid);
					out.println(" ");
					for (int mi = 0; mi < _iter; mi++)
					{
						_producer.send(getMesssage(gid, mi));
					}
					out.println("End sending MessageGroup-" + gid);
				}
				catch (Throwable e)
				{
					e.printStackTrace();
					System.exit(-1);
				}
			}
		};

		while (true)
		{
			exec.execute(messageSender);
			Thread.sleep(3000);
		}
	}

	private static TextMessage getMesssage(long mgid, long mi)
	{
		try
		{
			final TextMessage message = _auto_ack_session.createTextMessage();
			message.setJMSDeliveryMode(javax.jms.DeliveryMode.PERSISTENT);
			final String lixo = RandomStringUtils.randomAlphabetic(200);
			final String txt = mgid + "$MessageId-" + mi + "$Content: " + lixo;
			message.setText(txt);
			return message;
		}
		catch (Throwable e)
		{
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}
}
