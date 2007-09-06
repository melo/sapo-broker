package pt.com.gcs.messaging;

public enum Destination
{

	TOPIC(1), QUEUE(2);

	private final int _dtype;

	Destination(int dmode)
	{
		_dtype = dmode;
	}

	public int getValue()
	{
		return _dtype;
	}

	public static Destination lookup(int value)
	{
		if (value == 1)
		{
			return Destination.TOPIC;
		}
		return Destination.QUEUE;
	}
}
