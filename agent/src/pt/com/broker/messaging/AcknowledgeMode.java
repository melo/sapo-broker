package pt.com.broker.messaging;

public enum AcknowledgeMode
{

	AUTO(1), CLIENT(2);

	private final int _dmode;

	AcknowledgeMode(int dmode)
	{
		_dmode = dmode;
	}

	public int getValue()
	{
		return _dmode;
	}

	public static AcknowledgeMode lookup(int value)
	{
		if (value == 2)
		{
			return AcknowledgeMode.CLIENT;
		}
		return AcknowledgeMode.AUTO;
	}
}
