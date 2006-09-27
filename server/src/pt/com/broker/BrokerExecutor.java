package pt.com.broker;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BrokerExecutor
{
	private static final BrokerExecutor instance = new BrokerExecutor();

	private ThreadPoolExecutor exec_srv;
	
	private ThreadPoolExecutor io_srv;


	private final ScheduledThreadPoolExecutor shed_exec_srv;

	private BrokerExecutor()
	{
		RejectedExecutionHandler h = new ThreadPoolExecutor.CallerRunsPolicy();
		
		exec_srv = new ThreadPoolExecutor(10, 25, 30L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
		exec_srv.prestartAllCoreThreads();	
		exec_srv.setRejectedExecutionHandler(h);
		
		io_srv = new ThreadPoolExecutor(12, 12, 30L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
		io_srv.prestartAllCoreThreads();	
		io_srv.setRejectedExecutionHandler(h);		

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

	public static Executor getIOExecutor()
	{
		return instance.io_srv;
	}
}
