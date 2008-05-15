package pt.com.broker;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.caudexorigo.concurrent.CustomExecutors;

public class BrokerClientExecutor
{

	private static final BrokerClientExecutor instance = new BrokerClientExecutor();

	private ThreadPoolExecutor exec_srv;

	private final ScheduledThreadPoolExecutor shed_exec_srv;

	private BrokerClientExecutor()
	{
		exec_srv = CustomExecutors.newThreadPool(6, "BrokerClient-Async");

		shed_exec_srv = CustomExecutors.newScheduledThreadPool(3, "BrokerClient-Shed");
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
