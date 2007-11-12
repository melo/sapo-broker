package pt.com.gcs.messaging;

import pt.com.gcs.Gcs;

public class TopicToQueueDispatcher implements MessageListener
{

	private final String _queueName;

	public TopicToQueueDispatcher(String queueName)
	{
		_queueName = queueName;
	}

	public void onMessage(Message message)
	{
		message.setDestination(_queueName);
		Gcs.enqueue(message);
		Gcs.ackMessage(message.getMessageId());
	}
}
