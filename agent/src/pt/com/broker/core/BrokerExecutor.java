package pt.com.broker.core;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BrokerExecutor
{

	private static final int MINIMUM_POOL_SIZE = 10;

	private static final int NUM_CPUS = Runtime.getRuntime().availableProcessors();

	private static final int DEFAULT_POOL_SIZE = NUM_CPUS * MINIMUM_POOL_SIZE;

	private static final BrokerExecutor instance = new BrokerExecutor();

	private ThreadPoolExecutor exec_srv;

	private final ScheduledThreadPoolExecutor shed_exec_srv;

	private BrokerExecutor()
	{
		RejectedExecutionHandler h = new ThreadPoolExecutor.CallerRunsPolicy();

		exec_srv = new ThreadPoolExecutor(MINIMUM_POOL_SIZE, DEFAULT_POOL_SIZE, 30L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
		exec_srv.prestartAllCoreThreads();
		exec_srv.setRejectedExecutionHandler(h);

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
