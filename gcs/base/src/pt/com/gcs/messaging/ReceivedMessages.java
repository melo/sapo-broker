package pt.com.gcs.messaging;

import java.util.LinkedList;

public class ReceivedMessages
{
	private Object mutex = new Object();

	private LinkedList<String> messages = new LinkedList<String>();

	public ReceivedMessages()
	{
	}

	public final boolean isDuplicate(String msgId)
	{
		synchronized (mutex)
		{
			if (messages.contains(msgId))
			{
				System.out.println("ReceivedMessages.isDuplicate():true");
				return true;
			}
			else
			{
				System.out.println("ReceivedMessages.isDuplicate():false");

				messages.add(msgId);

				if (messages.size() > 100)
				{
					messages.removeFirst();
				}
				return false;
			}

		}
	}

}
