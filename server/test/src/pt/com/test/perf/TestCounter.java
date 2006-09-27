package pt.com.test.perf;

import java.util.concurrent.atomic.AtomicLong;

public class TestCounter
{
	public AtomicLong counter;

	public long start;

	public TestCounter(long initValue)
	{
		counter = new AtomicLong(initValue);
		start = System.currentTimeMillis();
	}

}
