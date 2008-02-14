package pt.com.gcs.messaging;


class TopicToQueueDispatcher implements MessageListener
{

	private final String _queueName;

	public TopicToQueueDispatcher(String queueName)
	{
		_queueName = queueName;
	}

	public boolean onMessage(Message message)
	{
		message.setDestination(_queueName);
		Gcs.enqueue(message);
		return true;
	}

	public String getQueueName()
	{
		return _queueName;
	}
}
