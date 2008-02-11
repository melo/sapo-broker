package pt.com.gcs.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueueCounter implements Runnable
{
	private static Logger log = LoggerFactory.getLogger(QueueCounter.class);

	@Override
	public void run()
	{
		for (QueueProcessor qp : QueueProcessorList.values())
		{
			long cnt = qp.getQueuedMessagesCount();
			if (cnt > 0)
			{
				if (qp.hasRecipient())
				{
					log.info("Queue '{}' has {} message(s).", qp.getDestinationName(), cnt);
				}
				else
				{
					log.warn("Operator attention required. Queue '{}' has {} message(s) and no consumers.", qp.getDestinationName(), cnt);
				}
			}
			else
			{
				log.info("Queue '{}' is empty.", qp.getDestinationName());
			}
		}
	}

}
