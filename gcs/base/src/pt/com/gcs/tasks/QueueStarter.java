package pt.com.gcs.tasks;

import pt.com.gcs.messaging.QueueProcessor;

public class QueueStarter implements Runnable
{
	private QueueProcessor _qp;

	public QueueStarter(QueueProcessor qp)
	{
		_qp = qp;
	}

	public void run()
	{
		_qp.wakeup();
	}

}
