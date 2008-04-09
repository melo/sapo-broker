package pt.com.broker.messaging;

import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.BrokerExecutor;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.gcs.messaging.Gcs;
import pt.com.gcs.messaging.Message;

public class BrokerSyncConsumer
{
	private static final Logger log = LoggerFactory.getLogger(BrokerSyncConsumer.class);

	public static void poll(Poll poll, IoSession ios)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Poll message from Queue '{}'", poll.destinationName);
		}

		try
		{
			Message m = Gcs.poll(poll.destinationName);
			if (m == null)
			{
				BrokerExecutor.schedule(new QueuePoller(poll, ios), 1000, TimeUnit.MILLISECONDS);
				return;
			}

			if ((ios != null) && ios.isConnected() && !ios.isClosing())
			{
				final SoapEnvelope response = BrokerListener.buildNotification(m, "queue");
				WriteFuture future = ios.write(response);
				future.awaitUninterruptibly(5000, TimeUnit.MILLISECONDS);

				if (future.isWritten())
				{
					if (log.isDebugEnabled())
					{
						log.debug("Delivered message: '{}'", m.getMessageId());
					}
				}
				else
				{
					Gcs.releaseMessage(poll.destinationName, m.getMessageId());
					if (log.isDebugEnabled())
					{
						log.debug("Release message: '{}'", m.getMessageId());
					}
				}
			}
			else
			{
				Gcs.removeSyncConsumer(poll.destinationName);
			}
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

}
