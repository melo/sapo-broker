package pt.com.broker.client.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.concurrent.CustomExecutors;
import org.caudexorigo.concurrent.Sleep;

import pt.com.broker.client.NetworkConnector;

public abstract class ProtocolHandler<T>
{
	private final AtomicBoolean _isReconnecting = new AtomicBoolean(false);

	private final ExecutorService exec = CustomExecutors.newScheduledThreadPool(1, "protocol-handler");

	private final ScheduledExecutorService shed_exec = CustomExecutors.newScheduledThreadPool(1, "sched-protocol-handler");

	private Object rlock = new Object();

	private Object wlock = new Object();

	public abstract T decode(DataInputStream in) throws IOException;

	public abstract void encode(T message, DataOutputStream out) throws IOException;

	public abstract void onConnectionClose();

	public abstract void onConnectionOpen();

	public abstract void onError(Throwable error);

	public abstract NetworkConnector getConnector();

	protected abstract void handleReceivedMessage(T request);

	private final Runnable reader = new Runnable()
	{
		public void run()
		{
			NetworkConnector connector = getConnector();
			DataInputStream in = connector.getInput();

			while (true)
			{

				try
				{
					T message = doDecode(in);
					handleReceivedMessage(message);
				}
				catch (Throwable error)
				{
					final Throwable rootCause = ErrorAnalyser.findRootCause(error);
					if (rootCause instanceof IOException)
					{
						Sleep.time(2000);
						connector.reconnect(rootCause);
						onConnectionOpen();
						in = connector.getInput();
					}
					else
					{
						try
						{
							onError(rootCause);
						}
						catch (Throwable t)
						{
							// ignore
						}
					}
				}
			}
		}
	};

	private Throwable resetConnection(final NetworkConnector connector, Throwable error)
	{
		final Throwable rootCause = ErrorAnalyser.findRootCause(error);
		if (rootCause instanceof IOException)
		{
			if (!_isReconnecting.getAndSet(true))
			{
				Runnable reconnector = new Runnable()
				{
					@Override
					public void run()
					{
						connector.reconnect(rootCause);
						_isReconnecting.set(false);
						onConnectionOpen();
					}
				};

				shed_exec.schedule(reconnector, 2000, TimeUnit.MILLISECONDS);
			}
		}
		return rootCause;
	}

	private T doDecode(DataInputStream in) throws IOException
	{
		synchronized (rlock)
		{
			return decode(in);
		}
	}

	public void doEncode(T message, DataOutputStream out) throws IOException
	{
		synchronized (wlock)
		{
			encode(message, out);
		}
	}

	public final void sendMessage(final T message) throws Throwable
	{
		final NetworkConnector connector = getConnector();
		try
		{
			DataOutputStream out = connector.getOutput();
			doEncode(message, out);
		}
		catch (Throwable error)
		{
			Throwable rootCause = resetConnection(connector, error);
			throw rootCause;
		}
	}

	public final void start() throws Throwable
	{
		exec.execute(reader);
	}

	public final void stop()
	{
		getConnector().close();

		try
		{
			exec.shutdown();
			shed_exec.shutdown();
		}
		catch (Throwable e)
		{
			// ignore
		}
	}
}
