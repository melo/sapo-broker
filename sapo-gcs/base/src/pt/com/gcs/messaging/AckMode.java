package pt.com.gcs.messaging;

public enum AckMode
{
	AUTO(1), CLIENT(2);

	private final int _ackm;

	AckMode(int ackm)
	{
		_ackm = ackm;
	}

	public int getValue()
	{
		return _ackm;
	}

	public static AckMode lookup(int value)
	{
		if (value == 1)
		{
			return AckMode.AUTO;
		}
		return AckMode.CLIENT;
	}
}
