package pt.com.manta;

import java.util.Iterator;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.mr.MantaAgent;
import org.mr.kernel.services.MantaService;
import org.mr.kernel.world.WorldModeler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.AcknowledgeMode;
import pt.com.broker.BrokerConsumer;
import pt.com.broker.MQ;
import pt.com.broker.Notify;
import pt.com.broker.TopicToQueueDispatcher;
import pt.com.text.StringUtils;

public class Dispatcher implements MessageListener
{
	private static Logger log = LoggerFactory.getLogger(Dispatcher.class);

	private MessageConsumer _dconsumer;

	public Dispatcher()
	{
		try
		{
			_dconsumer = BrokerConsumer.getInstance().getMessageConsumer(AcknowledgeMode.AUTO, "TOPIC", MQ.DISPATCHER_TOPIC);
			_dconsumer.setMessageListener(this);
			preFillDispatcher();
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public void onMessage(Message message)
	{
		try
		{
			TextMessage msg = (TextMessage) message;

			String topic_as_queue_name = msg.getText();
			Notify sb = new Notify();
			sb.destinationType = "TOPIC_AS_QUEUE";
			sb.destinationName = topic_as_queue_name;

			TopicToQueueDispatcher.add(sb);
		}
		catch (Throwable e)
		{
			log.error("ATTENTION!! THERE WILL BE NO MESSAGES DISPATCHED", e);
		}
	}

	public void preFillDispatcher()
	{
		WorldModeler world = MantaAgent.getInstance().getSingletonRepository().getWorldModeler();

		Iterator iter = world.getServices(world.getDefaultDomainName()).iterator();
		while (iter.hasNext())
		{
			MantaService service = (MantaService) iter.next();
			if (service.getServiceType() == MantaService.SERVICE_TYPE_QUEUE)
			{
				String queueName = service.getServiceName();
				if (StringUtils.contains(queueName, '@'))
				{
					Notify sb = new Notify();
					sb.destinationType = "TOPIC_AS_QUEUE";
					sb.destinationName = queueName;
					TopicToQueueDispatcher.add(sb);
				}
			}
		}
	}

}
