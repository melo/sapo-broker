package pt.com.broker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import pt.com.broker.messaging.BrokerMessage;

public class SyncConsumer
{
	private final AtomicInteger count = new AtomicInteger(0);
	private final BlockingQueue<BrokerMessage> queue = new LinkedBlockingQueue<BrokerMessage>();

	protected void increment()
	{
		count.incrementAndGet();
	}
	
	protected void decrement()
	{
		count.decrementAndGet();
	}

	protected int count()
	{
		return count.get();
	}

	protected BrokerMessage take()
	{
		try
		{
			return queue.take();
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}

	protected void offer(BrokerMessage msg)
	{
		queue.offer(msg);
	}

}
