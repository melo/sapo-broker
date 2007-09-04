package pt.com.gcs.messaging;

public enum MessageType
{
	COM_TOPIC(1), COM_QUEUE(2),

	SYSTEM_TOPIC(3), SYSTEM_QUEUE(4), ACK(5), PING(6), HELLO(7), UNDEF(0);

	private final int _mt;

	MessageType(int mt)
	{
		_mt = mt;
	}

	public int getValue()
	{
		return _mt;
	}

	public static MessageType lookup(int value)
	{
		if (value == 1)
		{
			return MessageType.COM_TOPIC;
		}
		else if (value == 2)
		{
			return MessageType.COM_QUEUE;
		}
		else if (value == 3)
		{
			return MessageType.SYSTEM_TOPIC;
		}
		else if (value == 4)
		{
			return MessageType.SYSTEM_QUEUE;
		}
		else if (value == 5)
		{
			return MessageType.ACK;
		}
		else if (value == 6)
		{
			return MessageType.PING;
		}
		else if (value == 7)
		{
			return MessageType.HELLO;
		}
		else
		{
			return MessageType.UNDEF;
		}
	}
}
