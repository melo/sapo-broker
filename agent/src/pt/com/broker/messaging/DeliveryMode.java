package pt.com.broker.messaging;

public enum DeliveryMode
{

	TRANSIENT(1), PERSISTENT(2);

	private final int _dmode;

	DeliveryMode(int dmode)
	{
		_dmode = dmode;
	}

	public int getValue()
	{
		return _dmode;
	}

	public static DeliveryMode lookup(int value)
	{
		if (value == 2)
		{
			return DeliveryMode.PERSISTENT;
		}
		return DeliveryMode.TRANSIENT;
	}
}
