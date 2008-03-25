package pt.com.gcs.messaging;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueueCounter implements Runnable
{
	private static Logger log = LoggerFactory.getLogger(QueueCounter.class);

	@Override
	public void run()
	{
		Collection<QueueProcessor> qpl = QueueProcessorList.values();
		
		log.debug("Number of registered Queues: {}", qpl.size());
		
		for (QueueProcessor qp : qpl)
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
			else if (cnt == 0)
			{
				log.info("Queue '{}' is empty.", qp.getDestinationName());
			}
		}
	}

}
