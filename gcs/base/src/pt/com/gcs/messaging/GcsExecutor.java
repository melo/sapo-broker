package pt.com.gcs.messaging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.caudexorigo.concurrent.CustomExecutors;

public class GcsExecutor
{
	private static final GcsExecutor instance = new GcsExecutor();

	private ExecutorService exec_srv;

	private final ScheduledThreadPoolExecutor shed_exec_srv;

	private GcsExecutor()
	{
		exec_srv = CustomExecutors.newThreadPool(16, "GCS-Async");

		shed_exec_srv = CustomExecutors.newScheduledThreadPool(10, "GCS-Sched");
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
