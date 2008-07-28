package pt.com.gcs.messaging;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.GcsInfo;

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
				log.info("Queue '{}' has {} message(s).", qp.getDestinationName(), cnt);
			}
			else if ((cnt == 0) && !qp.emptyQueueInfoDisplay.getAndSet(true))
			{
				log.info("Queue '{}' is empty.", qp.getDestinationName());
			}

			try
			{
				Message cnt_message = new Message();
				String dName = String.format("/system/stats/queue-size/#%s#", qp.getDestinationName());
				String content = GcsInfo.getAgentName() + "#" + qp.getDestinationName() + "#" + cnt;
				cnt_message.setDestination(dName);
				cnt_message.setContent(content);

				Gcs.publish(cnt_message);
			}
			catch (Throwable error)
			{
				String emsg = String.format("Could not publish queue counter for '{}'", qp.getDestinationName());
				log.error(emsg, error);
			}
		}
	}
}
