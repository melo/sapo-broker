package pt.com.broker.core;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.caudexorigo.concurrent.CustomExecutors;

public class BrokerExecutor
{

	private static final BrokerExecutor instance = new BrokerExecutor();

	private ThreadPoolExecutor exec_srv;

	private final ScheduledThreadPoolExecutor shed_exec_srv;

	private BrokerExecutor()
	{
		exec_srv = CustomExecutors.newThreadPool(10);

		shed_exec_srv = new ScheduledThreadPoolExecutor(3);
	}

	public static void execute(Runnable task)
	{
		instance.exec_srv.execute(task);
	}

	public static void scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit)
	{
		instance.shed_exec_srv.scheduleAtFixedRate(task, initialDelay, period, unit);
	}

	public static void scheduleWithFixedDelay(Runnable task, long initialDelay, long period, TimeUnit unit)
	{
		instance.shed_exec_srv.scheduleWithFixedDelay(task, initialDelay, period, unit);
	}

	public static void schedule(Runnable task, long delay, TimeUnit unit)
	{
		instance.shed_exec_srv.schedule(task, delay, unit);
	}
}
