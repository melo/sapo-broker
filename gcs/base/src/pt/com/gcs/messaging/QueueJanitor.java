package pt.com.gcs.messaging;

import java.util.concurrent.TimeUnit;

import pt.com.gcs.tasks.GcsExecutor;

public class QueueJanitor
{
	private QueueJanitor()
	{
		final Runnable cleaner = new Runnable()
		{
			public void run()
			{
				// clean
			}
		};
		GcsExecutor.scheduleWithFixedDelay(cleaner, 30, 30, TimeUnit.SECONDS);
	}

}
