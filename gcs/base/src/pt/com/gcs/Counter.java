package pt.com.gcs;

import java.util.concurrent.atomic.AtomicLong;

public class Counter
{
	private final AtomicLong total;

	private final AtomicLong parcial;

	private final long startTimestamp;

	public Counter()
	{
		startTimestamp = System.currentTimeMillis();
		total = new AtomicLong(0L);
		parcial = new AtomicLong(0L);
	}

	public void increment()
	{
		total.incrementAndGet();
		parcial.incrementAndGet();
	}

	public void reset()
	{
		parcial.set(0L);
	}

	public long getTotal()
	{
		return total.get();
	}

	public long getParcial()
	{
		return parcial.get();
	}

	public long getStartTimestamp()
	{
		return startTimestamp;
	}
}
